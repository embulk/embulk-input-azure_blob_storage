package org.embulk.input.azure_blob_storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.util.Pages;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class TestAzureBlobStorageFileInputPlugin
{
    private static String AZURE_ACCOUNT_NAME;
    private static String AZURE_ACCOUNT_KEY;
    private static String AZURE_CONTAINER;
    private static String AZURE_CONTAINER_IMPORT_DIRECTORY;
    private static String AZURE_PATH_PREFIX;
    private FileInputRunner runner;
    private MockPageOutput output;

    /*
     * This test case requires environment variables
     *   AZURE_ACCOUNT_NAME
     *   AZURE_ACCOUNT_KEY
     *   AZURE_CONTAINER
     *   AZURE_CONTAINER_IMPORT_DIRECTORY
     */
    @BeforeClass
    public static void initializeConstant()
    {
        AZURE_ACCOUNT_NAME = System.getenv("AZURE_ACCOUNT_NAME");
        AZURE_ACCOUNT_KEY = System.getenv("AZURE_ACCOUNT_KEY");
        AZURE_CONTAINER = System.getenv("AZURE_CONTAINER");
        // skip test cases, if environment variables are not set.
        assumeNotNull(AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER);

        AZURE_CONTAINER_IMPORT_DIRECTORY = System.getenv("AZURE_CONTAINER_IMPORT_DIRECTORY") != null ? getDirectory(System.getenv("AZURE_CONTAINER_IMPORT_DIRECTORY")) : getDirectory("");
        AZURE_PATH_PREFIX = AZURE_CONTAINER_IMPORT_DIRECTORY + "sample_";
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private ConfigSource config;
    private AzureBlobStorageFileInputPlugin plugin;

    @Before
    public void createResources() throws GeneralSecurityException, NoSuchMethodException, IOException
    {
        config = config();
        plugin = new AzureBlobStorageFileInputPlugin();
        runner = new FileInputRunner(runtime.getInstance(AzureBlobStorageFileInputPlugin.class));
        output = new MockPageOutput();
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", "my-prefix");

        PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(5000, task.getMaxResults());
        assertEquals(5, task.getMaxConnectionRetry());
    }

    public ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesAccountNameIsNull()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("account_name", null)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesAccountKeyIsNull()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", null)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesContainerIsNull()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", null)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        runner.transaction(config, new Control());
    }

    @Test
    public void testAzureClientCreateSuccessfully()
            throws GeneralSecurityException, IOException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        PluginTask task = config().loadConfig(PluginTask.class);

        Method method = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        method.invoke(plugin, task.getAccountName(), task.getAccountKey()); // no errors happens
    }

    @Test
    public void testResume()
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        task.setFiles(Arrays.asList("in/aa/a"));
        ConfigDiff configDiff = plugin.resume(task.dump(), 0, new FileInputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                return emptyTaskReports(taskCount);
            }
        });
        assertEquals("in/aa/a", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testCleanup()
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testListFiles()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        List<String> expected = Arrays.asList(
                AZURE_CONTAINER_IMPORT_DIRECTORY + "sample_01.csv",
                AZURE_CONTAINER_IMPORT_DIRECTORY + "sample_02.csv"
        );

        PluginTask task = config.loadConfig(PluginTask.class);
        ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertEquals(2, taskCount);
                return emptyTaskReports(taskCount);
            }
        });

        Method newAzureClient = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        newAzureClient.setAccessible(true);
        CloudBlobClient client = (CloudBlobClient) newAzureClient.invoke(plugin, task.getAccountName(), task.getAccountKey());

        Method listFiles = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("listFiles", CloudBlobClient.class, PluginTask.class);
        listFiles.setAccessible(true);
        List<String> actual = (List<String>) listFiles.invoke(plugin, client, task);
        assertEquals(expected, actual);
        assertEquals(AZURE_CONTAINER_IMPORT_DIRECTORY + "sample_02.csv", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testAzureFileInputByOpen()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        runner.transaction(config, new Control());

        Method newAzureClient = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        newAzureClient.setAccessible(true);
        CloudBlobClient client = (CloudBlobClient) newAzureClient.invoke(plugin, task.getAccountName(), task.getAccountKey());

        Method listFiles = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("listFiles", CloudBlobClient.class, PluginTask.class);
        listFiles.setAccessible(true);
        task.setFiles((List<String>) listFiles.invoke(plugin, client, task));

        assertRecords(config, output);
    }

    static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(Exec.newTaskReport());
        }
        return reports.build();
    }

    private class Control
            implements InputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource, Schema schema, int taskCount)
        {
            List<TaskReport> reports = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                reports.add(runner.run(taskSource, schema, i, output));
            }
            return reports;
        }
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        builder.add(ImmutableMap.of("name", "json_column", "type", "json"));
        return builder.build();
    }

    private void assertRecords(ConfigSource config, MockPageOutput output)
    {
        List<Object[]> records = getRecords(config, output);
        assertEquals(10, records.size());
        {
            Object[] record = records.get(0);
            assertEquals(1L, record[0]);
            assertEquals(32864L, record[1]);
            assertEquals("2015-01-27 19:23:49 UTC", record[2].toString());
            assertEquals("2015-01-27 00:00:00 UTC", record[3].toString());
            assertEquals("embulk", record[4]);
            assertEquals("{\"k\":true}", record[5].toString());
        }

        {
            Object[] record = records.get(1);
            assertEquals(2L, record[0]);
            assertEquals(14824L, record[1]);
            assertEquals("2015-01-27 19:01:23 UTC", record[2].toString());
            assertEquals("2015-01-27 00:00:00 UTC", record[3].toString());
            assertEquals("embulk jruby", record[4]);
            assertEquals("{\"k\":1}", record[5].toString());
        }

        {
            Object[] record = records.get(2);
            assertEquals("{\"k\":1.23}", record[5].toString());
        }

        {
            Object[] record = records.get(3);
            assertEquals("{\"k\":\"v\"}", record[5].toString());
        }

        {
            Object[] record = records.get(4);
            assertEquals("{\"k\":\"2015-02-03 08:13:45\"}", record[5].toString());
        }
    }

    private List<Object[]> getRecords(ConfigSource config, MockPageOutput output)
    {
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        return Pages.toObjects(schema, output.pages);
    }

    private static String getDirectory(String dir)
    {
        if (!dir.isEmpty() && !dir.endsWith("/")) {
            dir = dir + "/";
        }
        if (dir.startsWith("/")) {
            dir = dir.replaceFirst("/", "");
        }
        return dir;
    }
}
