package org.embulk.input.azure_blob_storage;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.PluginTask;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
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

import static org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.CONFIG_MAPPER;
import static org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.CONFIG_MAPPER_FACTORY;
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
        ConfigSource config = runtime.getExec().newConfigSource()
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", "my-prefix");

        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertEquals(5000, task.getMaxResults());
        assertEquals(10, task.getMaxConnectionRetry());
    }

    @Test(expected = ConfigException.class)
    public void checkDefaultValuesAccountNameIsNull()
    {
        ConfigSource config = runtime.getExec().newConfigSource()
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
        ConfigSource config = runtime.getExec().newConfigSource()
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
        ConfigSource config = runtime.getExec().newConfigSource()
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
        PluginTask task = CONFIG_MAPPER.map(config(), PluginTask.class);

        Method method = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        method.invoke(plugin, task.getAccountName(), task.getAccountKey()); // no errors happens
    }

    @Test
    public void testResume()
        throws IOException
    {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        task.setFiles(createFileList(Arrays.asList("in/aa/a"), task));
        ConfigDiff configDiff = plugin.resume(task.toTaskSource(), 0, new FileInputPlugin.Control()
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
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        plugin.cleanup(task.toTaskSource(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testListFiles()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        List<String> expected = Arrays.asList(
            AZURE_CONTAINER_IMPORT_DIRECTORY + "sample_01.csv",
            AZURE_CONTAINER_IMPORT_DIRECTORY + "sample_02.csv"
        );

        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        ConfigDiff configDiff = plugin.transaction(config, new FileInputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource, int taskCount)
            {
                assertEquals(2, taskCount);
                return emptyTaskReports(taskCount);
            }
        });

        Method listFiles = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("listFiles", PluginTask.class);
        listFiles.setAccessible(true);
        FileList actual = (FileList) listFiles.invoke(plugin, task);
        assertEquals(expected.get(0), actual.get(0).get(0));
        assertEquals(expected.get(1), actual.get(1).get(0));
        assertEquals(AZURE_CONTAINER_IMPORT_DIRECTORY + "sample_02.csv", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testAzureFileInputByOpen()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException
    {
        ConfigSource newConfig = runtime.getExec().newConfigSource().merge(config);
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        runner.transaction(newConfig, new Control());

        Method listFiles = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("listFiles", PluginTask.class);
        listFiles.setAccessible(true);
        task.setFiles((FileList) listFiles.invoke(plugin, task));

        assertRecords(config, output);
    }

    @Test
    public void testAzureIncrementalWithoutDuplicate()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException
    {
        // This is temperately solution to fix  org.embulk.util.config.DataSourceImpl does not implement loadConfig.
        ConfigSource newConfig = runtime.getExec().newConfigSource().merge(config);
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        ConfigDiff configDiff = runner.transaction(newConfig, new Control());

        Method listFiles = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("listFiles", PluginTask.class);
        listFiles.setAccessible(true);
        task.setFiles((FileList) listFiles.invoke(plugin, task));

        assertRecords(config, output);

        output.pages.clear();

        config.set("last_path", configDiff.get(String.class, "last_path"));
        newConfig = runtime.getExec().newConfigSource().merge(config);
        task = CONFIG_MAPPER.map(config, PluginTask.class);
        runner.transaction(newConfig, new Control());
        task.setFiles((FileList) listFiles.invoke(plugin, task));
        List<Object[]> records = getRecords(config, output);
        assertEquals(0, records.size());
    }

    @Test
    public void testCreateNextToken() throws Exception
    {
        Method base64Encode = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("createNextToken", String.class);
        base64Encode.setAccessible(true);

        String expected = "2!92!MDAwMDI1IXJlYWRvbmx5L3NhbXBsZV8wMS50c3YuZ3ohMDAwMDI4ITk5OTktMTItMzFUMjM6NTk6NTkuOTk5OTk5OVoh";
        String lastPath = "readonly/sample_01.tsv.gz";
        assertEquals(expected, base64Encode.invoke(plugin, lastPath).toString());

        expected = "2!120!MDAwMDQ2IXBhdGgvdGhhdC9oYXZlL2xvbmcvcGF0aC9uYW1lL3NhbXBsZV8wMS50c3YuZ3ohMDAwMDI4ITk5OTktMTItMzFUMjM6NTk6NTkuOTk5OTk5OVoh";
        lastPath = "path/that/have/long/path/name/sample_01.tsv.gz";
        assertEquals(expected, base64Encode.invoke(plugin, lastPath).toString());
    }

    static List<TaskReport> emptyTaskReports(int taskCount)
    {
        ImmutableList.Builder<TaskReport> reports = new ImmutableList.Builder<>();
        for (int i = 0; i < taskCount; i++) {
            reports.add(CONFIG_MAPPER_FACTORY.newTaskReport());
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

    public ConfigSource config()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));
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
        Schema.Builder builder = Schema.builder();
        config.getNested("parser").get(ArrayNode.class, "columns").forEach(
            column -> builder.add(column.get("name").asText(), geType(column.get("type").asText()))
        );
        Schema schema = builder.build();
        return Pages.toObjects(schema, output.pages);
    }

    private Type geType(String typeName)
    {
        switch (typeName.toLowerCase()) {
            case "string": return Types.STRING;
            case "long": return Types.LONG;
            case "double": return Types.DOUBLE;
            case "timestamp": return Types.TIMESTAMP;
            case "boolean": return Types.BOOLEAN;
            case "json": return Types.JSON;
        }
        throw new ConfigException("Unsupported type " + typeName);
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

    private FileList createFileList(List<String> fileList, PluginTask task)
        throws IOException
    {
        FileList.Builder builder = new FileList.Builder(task);
        for (String file : fileList) {
            builder.add(file, 0);
        }
        return builder.build();
    }
}
