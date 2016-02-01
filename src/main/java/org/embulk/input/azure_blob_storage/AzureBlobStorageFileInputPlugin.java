package org.embulk.input.azure_blob_storage;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.ResultContinuation;
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
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AzureBlobStorageFileInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("account_name")
        String getAccountName();

        @Config("account_key")
        String getAccountKey();

        @Config("container")
        String getContainer();

        @Config("path_prefix")
        String getPathPrefix();

        @Config("last_path")
        @ConfigDefault("null")
        Optional<String> getLastPath();

        @Config("max_results")
        @ConfigDefault("5000")
        int getMaxResults();

        List<String> getFiles();

        void setFiles(List<String> files);

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    private static final Logger log = Exec.getLogger(AzureBlobStorageFileInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        final PluginTask task = config.loadConfig(PluginTask.class);

        CloudBlobClient blobClient = newAzureClient(task.getAccountName(), task.getAccountKey());
        task.setFiles(listFiles(blobClient, task));

        return resume(task.dump(), task.getFiles().size(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileInputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        control.run(taskSource, taskCount);

        ConfigDiff configDiff = Exec.newConfigDiff();

        List<String> files = new ArrayList<>(task.getFiles());
        if (files.isEmpty()) {
            if (task.getLastPath().isPresent()) {
                configDiff.set("last_path", task.getLastPath().get());
            }
        }
        else {
            Collections.sort(files);
            configDiff.set("last_path", files.get(files.size() - 1));
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

    private List<String> listFiles(CloudBlobClient client, PluginTask task)
    {
        if (task.getPathPrefix().equals("/")) {
            log.info("Listing files with prefix \"/\". This doesn't mean all files in a bucket. If you intend to read all files, use \"path_prefix: ''\" (empty string) instead.");
        }

        return listFilesWithPrefix(client, task.getContainer(), task.getPathPrefix(), task.getLastPath(), task.getMaxResults());
    }

    private static List<String> listFilesWithPrefix(CloudBlobClient client, String containerName,
                                             String prefix, Optional<String> lastPath, int maxResults)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        // It seems I can't cast lastKey<String> to token<ResultContinuation> by Azure SDK for Java
        String lastKey = lastPath.orNull();
        ResultContinuation token = null;

        try {
            CloudBlobContainer container = client.getContainerReference(containerName);
            ResultSegment<ListBlobItem> blobs;
            do {
                blobs = container.listBlobsSegmented(prefix, true, null, maxResults, token, null, null);
                log.debug(String.format("result count(include directory):%s continuationToken:%s", blobs.getLength(), blobs.getContinuationToken()));
                for (ListBlobItem blobItem : blobs.getResults()) {
                    if (blobItem instanceof CloudBlob) {
                        CloudBlob blob = (CloudBlob) blobItem;
                        if (blob.exists() && !blob.getUri().toString().endsWith("/")) {
                            builder.add(blob.getName());
                            log.debug(String.format("name:%s, class:%s, uri:%s", blob.getName(), blob.getClass(), blob.getUri()));
                        }
                    }
                }
                token = blobs.getContinuationToken();
            } while (blobs.getContinuationToken() != null);
        }
        catch (URISyntaxException | StorageException ex) {
            throw Throwables.propagate(ex);
        }
        return builder.build();
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
        private final String key;
        private boolean opened = false;

        public SingleFileProvider(PluginTask task, int taskIndex)
        {
            this.client = newAzureClient(task.getAccountName(), task.getAccountKey());
            this.containerName = task.getContainer();
            this.key = task.getFiles().get(taskIndex);
        }

        @Override
        public InputStream openNext() throws IOException
        {
            if (opened) {
                return null;
            }
            opened = true;
            InputStream inputStream = null;
            try {
                CloudBlobContainer container = client.getContainerReference(containerName);
                CloudBlob blob = container.getBlockBlobReference(key);
                inputStream = blob.openInputStream();
            }
            catch (StorageException | URISyntaxException ex) {
                Throwables.propagate(ex);
            }
            return inputStream;
        }

        @Override
        public void close() {}
    }
}
