package org.embulk.input.azure_blob_storage;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultContinuationType;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.file.InputStreamFileInput;
import org.embulk.util.file.InputStreamFileInput.InputStreamWithHints;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        @Config("proxy")
        @ConfigDefault("null")
        Optional<ProxyTask> getProxy();

        FileList getFiles();
        void setFiles(FileList files);
    }

    private static final Logger log = LoggerFactory.getLogger(AzureBlobStorageFileInputPlugin.class);

    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory
        .builder()
        .addDefaultModules()
        .build();
    public static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        if (task.getProxy().isPresent()) {
            System.setProperty("jdk.http.auth.tunneling.disabledSchemes", "");
        }
        task.setFiles(listFiles(task));

        return resume(task.toTaskSource(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileInputPlugin.Control control)
    {
        PluginTask task = CONFIG_MAPPER_FACTORY.createTaskMapper().map(taskSource, PluginTask.class);
        control.run(taskSource, taskCount);

        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();
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
                                    task.getLastPath(), task.getMaxResults(), task.getMaxConnectionRetry(), task.getProxy().orElse(null));
    }

    private static FileList listFilesWithPrefix(final FileList.Builder builder, final String accountName, final String accountKey,
                                                final String containerName, final String prefix, final Optional<String> lastPath,
                                                final int maxResults, final int maxConnectionRetry, ProxyTask proxyTask)
    {
        final String lastKey = (lastPath.isPresent() && !lastPath.get().isEmpty()) ? createNextToken(lastPath.get()) : null;
        final OperationContext op = ProxyTask.toOperationContext(proxyTask);
        try {
            return RetryExecutor.builder()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build().runInterruptible(new Retryable<FileList>() {
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
                                blobs = container.listBlobsSegmented(prefix, true, null, maxResults, token, null, op);
                                log.debug(String.format("result count(include directory):%s continuationToken:%s", blobs.getLength(), blobs.getContinuationToken()));
                                for (ListBlobItem blobItem : blobs.getResults()) {
                                    if (blobItem instanceof CloudBlob) {
                                        CloudBlob blob = (CloudBlob) blobItem;
                                        if (blob.exists() && !blob.getUri().toString().endsWith("/")) {
                                            if (lastPath.isPresent()
                                                    && !lastPath.get().isEmpty()
                                                    && blob.getName().equals(lastPath.get())) {
                                                log.info("skip {} because match with last_path {}", blob.getName(), lastPath.get());
                                                continue;
                                            }
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
                            String message = String.format("Azure GET request failed. Retrying %d/%d after %d seconds. Message: %s",
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
            throw new DataException(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw new DataException(ex);
        }
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = CONFIG_MAPPER_FACTORY.createTaskMapper().map(taskSource, PluginTask.class);
        return new AzureFileInput(task, taskIndex);
    }

    class AzureFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {
        public AzureFileInput(PluginTask task, int taskIndex)
        {
            super(Exec.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }
        public void abort() {}

        public TaskReport commit()
        {
            return CONFIG_MAPPER_FACTORY.newTaskReport();
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
        private final OperationContext op;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newAzureClient(task.getAccountName(), task.getAccountKey());
            this.containerName = task.getContainer();
            this.iterator = task.getFiles().get(taskIndex).iterator();
            this.maxConnectionRetry = task.getMaxConnectionRetry();
            this.op = ProxyTask.toOperationContext(task.getProxy().orElse(null));
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
                return  RetryExecutor.builder()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build().runInterruptible(new Retryable<InputStreamWithHints>() {
                            @Override
                            public InputStreamFileInput.InputStreamWithHints call() throws StorageException, URISyntaxException, IOException
                            {
                                CloudBlobContainer container = client.getContainerReference(containerName);
                                CloudBlob blob = container.getBlockBlobReference(key);
                                return new InputStreamFileInput.InputStreamWithHints(
                                        blob.openInputStream(0, null, null, null, op),
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
                throw new DataException(ex.getCause());
            }
            catch (InterruptedException ex) {
                throw new DataException(ex);
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
