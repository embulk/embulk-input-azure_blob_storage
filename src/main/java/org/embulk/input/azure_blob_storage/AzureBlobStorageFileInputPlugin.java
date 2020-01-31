package org.embulk.input.azure_blob_storage;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultContinuationType;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamFileInput.InputStreamWithHints;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class AzureBlobStorageFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task, FileList.Task
    {
        @Config("account_name")
        String getAccountName();

        @Config("account_key")
        String getAccountKey();

        @Config("container")
        String getContainer();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("incremental")
        @ConfigDefault("true")
        boolean getIncremental();

        @Config("last_path")
        @ConfigDefault("null")
        Optional<String> getLastPath();

        @Config("max_results")
        @ConfigDefault("5000")
        int getMaxResults();

        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect Azure Blob Storage if failed.
        int getMaxConnectionRetry();

        FileList getFiles();
        void setFiles(FileList files);

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    private static final Logger log = Exec.getLogger(AzureBlobStorageFileInputPlugin.class);

    private static boolean IS_REMOVE_FIRST_RECORD = true;

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        task.setFiles(listFiles(task));

        return resume(task.dump(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileInputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();
        if (task.getIncremental()) {
            configDiff.set("last_path", task.getFiles().getLastPath(task.getLastPath()));
        }

        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    private static CloudBlobClient newAzureClient(String accountName, String accountKey)
    {
        String connectionString = "DefaultEndpointsProtocol=https;" +
                "AccountName=" + accountName + ";" +
                "AccountKey=" + accountKey;

        CloudStorageAccount account;
        try {
            account = CloudStorageAccount.parse(connectionString);
        }
        catch (InvalidKeyException | URISyntaxException ex) {
            throw new ConfigException(ex);
        }
        return account.createCloudBlobClient();
    }

    private FileList listFiles(PluginTask task)
    {
        if (task.getPathPrefix().equals("/")) {
            log.info("Listing files with prefix \"/\". This doesn't mean all files in a bucket. If you intend to read all files, use \"path_prefix: ''\" (empty string) instead.");
        }
        FileList.Builder builder = new FileList.Builder(task);

        return listFilesWithPrefix(builder, task.getAccountName(), task.getAccountKey(), task.getContainer(), task.getPathPrefix(),
                                    task.getLastPath(), task.getMaxResults(), task.getMaxConnectionRetry());
    }

    private static FileList listFilesWithPrefix(final FileList.Builder builder, final String accountName, final String accountKey,
                                                final String containerName, final String prefix, final Optional<String> lastPath,
                                                final int maxResults, final int maxConnectionRetry)
    {
        final String lastKey = (lastPath.isPresent() && !lastPath.get().isEmpty()) ? createNextToken(lastPath.get()) : null;
        try {
            return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(new Retryable<FileList>() {
                        @Override
                        public FileList call() throws StorageException, URISyntaxException, IOException
                        {
                            CloudBlobClient client = newAzureClient(accountName, accountKey);
                            ResultContinuation token = null;
                            if (lastKey != null) {
                                token = new ResultContinuation();
                                token.setContinuationType(ResultContinuationType.BLOB);
                                log.debug("lastPath: {}", lastPath.get());
                                log.debug("lastPath(Base64encoded): {}", lastKey);
                                token.setNextMarker(lastKey);
                            }

                            CloudBlobContainer container = client.getContainerReference(containerName);
                            ResultSegment<ListBlobItem> blobs;
                            do {
                                blobs = container.listBlobsSegmented(prefix, true, null, maxResults, token, null, null);
                                log.debug(String.format("result count(include directory):%s continuationToken:%s", blobs.getLength(), blobs.getContinuationToken()));
                                if (lastKey != null && !blobs.getResults().isEmpty() && IS_REMOVE_FIRST_RECORD) {
                                    log.info("Remove first item " + blobs.getResults().remove(0).getStorageUri().getPrimaryUri());
                                    IS_REMOVE_FIRST_RECORD = false;
                                }
                                for (ListBlobItem blobItem : blobs.getResults()) {
                                    if (blobItem instanceof CloudBlob) {
                                        CloudBlob blob = (CloudBlob) blobItem;
                                        if (blob.exists() && !blob.getUri().toString().endsWith("/")) {
                                            builder.add(blob.getName(), blob.getProperties().getLength());
                                            log.debug(String.format("name:%s, class:%s, uri:%s", blob.getName(), blob.getClass(), blob.getUri()));
                                        }
                                    }
                                }
                                token = blobs.getContinuationToken();
                            } while (blobs.getContinuationToken() != null);
                            return builder.build();
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            String message = String.format("SFTP GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                                throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new AzureFileInput(task, taskIndex);
    }

    class AzureFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public AzureFileInput(PluginTask task, int taskIndex)
        {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }
        public void abort() {}

        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }

        @Override
        public void close() {}
    }

    class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private CloudBlobClient client;
        private final String containerName;
        private final Iterator<String> iterator;
        private final int maxConnectionRetry;
        private boolean opened = false;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newAzureClient(task.getAccountName(), task.getAccountKey());
            this.containerName = task.getContainer();
            this.iterator = task.getFiles().get(taskIndex).iterator();
            this.maxConnectionRetry = task.getMaxConnectionRetry();
        }

        @Override
        public InputStreamWithHints openNextWithHints()
        {
            if (opened || !iterator.hasNext()) {
                return null;
            }
            final String key = iterator.next();
            opened = true;
            try {
                return retryExecutor()
                        .withRetryLimit(maxConnectionRetry)
                        .withInitialRetryWait(500)
                        .withMaxRetryWait(30 * 1000)
                        .runInterruptible(new Retryable<InputStreamWithHints>() {
                            @Override
                            public InputStreamWithHints call() throws StorageException, URISyntaxException, IOException
                            {
                                CloudBlobContainer container = client.getContainerReference(containerName);
                                CloudBlob blob = container.getBlockBlobReference(key);
                                return new InputStreamWithHints(
                                        blob.openInputStream(),
                                        String.format("%s/%s", containerName, key)
                                );
                            }

                            @Override
                            public boolean isRetryableException(Exception exception)
                            {
                                return true;
                            }

                            @Override
                            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                    throws RetryGiveupException
                            {
                                String message = String.format("Azure Blob Storage GET request failed. Retrying %d/%d after %d seconds. Message: %s",
                                        retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                                if (retryCount % 3 == 0) {
                                    log.warn(message, exception);
                                }
                                else {
                                    log.warn(message);
                                }
                            }

                            @Override
                            public void onGiveup(Exception firstException, Exception lastException)
                                    throws RetryGiveupException
                            {
                            }
                        });
            }
            catch (RetryGiveupException ex) {
                throw Throwables.propagate(ex.getCause());
            }
            catch (InterruptedException ex) {
                throw Throwables.propagate(ex);
            }
        }

        @Override
        public void close() {}
    }

    private static String createNextToken(String path)
    {
        StringBuilder sb = new StringBuilder()
                .append(String.format("%06d", path.length()))
                .append("!")
                .append(path)
                .append("!000028!9999-12-31T23:59:59.9999999Z!");

        String encodedString = BaseEncoding.base64().encode(sb.toString().getBytes(Charsets.UTF_8));

        StringBuilder marker = new StringBuilder()
                .append("2")
                .append("!")
                .append(encodedString.length())
                .append("!")
                .append(encodedString);
        return marker.toString();
    }
}
