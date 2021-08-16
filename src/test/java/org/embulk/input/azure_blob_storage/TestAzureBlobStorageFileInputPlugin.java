package org.embulk.input.azure_blob_storage;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.PluginTask;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.embulk.test.TestingEmbulk;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.CONFIG_MAPPER;
import static org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

public class TestAzureBlobStorageFileInputPlugin
{
    private static String AZURE_ACCOUNT_NAME;
    private static String AZURE_ACCOUNT_KEY;
    private static String AZURE_CONTAINER;
    private static String AZURE_CONTAINER_IMPORT_DIRECTORY;
    private static String AZURE_PATH_PREFIX;

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

    private TestingEmbulk embulk = TestingEmbulk.builder()
            .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
            .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
            .registerPlugin(FileInputPlugin.class, "azure_blob_storage", AzureBlobStorageFileInputPlugin.class)
            .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
            .build();

    private ConfigSource config;
    private AzureBlobStorageFileInputPlugin plugin;

    @Before
    public void createResources()
    {
        config = config();
        plugin = new AzureBlobStorageFileInputPlugin();
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = embulk.newConfig()
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", "my-prefix");

        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertEquals(5000, task.getMaxResults());
        assertEquals(10, task.getMaxConnectionRetry());
    }

    @Test
    public void checkDefaultValuesAccountNameIsNull() throws IOException
    {
        ConfigSource config = embulk.newConfig()
                .set("type", "azure_blob_storage")
                .set("account_name", null)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        runInputAndExpect(config, "Field 'account_name' is required but not set.");
    }

    @Test
    public void checkDefaultValuesAccountKeyIsNull() throws IOException
    {
        ConfigSource config = embulk.newConfig()
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", null)
                .set("container", AZURE_CONTAINER)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        runInputAndExpect(config, "Field 'account_key' is required but not set.");
    }

    @Test
    public void checkDefaultValuesContainerIsNull() throws IOException
    {
        ConfigSource config = embulk.newConfig()
                .set("type", "azure_blob_storage")
                .set("account_name", AZURE_ACCOUNT_NAME)
                .set("account_key", AZURE_ACCOUNT_KEY)
                .set("container", null)
                .set("path_prefix", AZURE_PATH_PREFIX)
                .set("last_path", "")
                .set("parser", parserConfig(schemaConfig()));

        runInputAndExpect(config, "Field 'container' is required but not set.");
    }

    @Test
    public void testAzureClientCreateSuccessfully()
            throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException
    {
        PluginTask task = CONFIG_MAPPER.map(config(), PluginTask.class);

        Method method = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("newAzureClient", String.class, String.class);
        method.setAccessible(true);
        method.invoke(plugin, task.getAccountName(), task.getAccountKey()); // no errors happens
    }

    @Test
    public void testResume()
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
        plugin.cleanup(task.toTaskSource(), 0, Lists.newArrayList()); // no errors happens
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
        ConfigDiff configDiff = plugin.transaction(config, (taskSource, taskCount) -> {
            assertEquals(2, taskCount);
            return emptyTaskReports(taskCount);
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
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, URISyntaxException
    {
        ConfigSource newConfig = embulk.newConfig().merge(config);
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        final Path out = embulk.createTempFile("csv");

        embulk.runInput(newConfig, out);

        Method listFiles = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("listFiles", PluginTask.class);
        listFiles.setAccessible(true);
        task.setFiles((FileList) listFiles.invoke(plugin, task));

        assertArrayEquals(Files.readAllBytes(Paths.get(Resources.getResource("sample_out.csv").toURI())), Files.readAllBytes(out));
    }

    @Test
    public void testAzureIncrementalWithoutDuplicate()
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, IOException, URISyntaxException
    {
        ConfigSource newConfig = embulk.newConfig().merge(config);
        PluginTask task = CONFIG_MAPPER.map(newConfig, PluginTask.class);
        Path out = embulk.createTempFile("csv");

        TestingEmbulk.RunResult runInputResult = embulk.runInput(newConfig, out);

        Method listFiles = AzureBlobStorageFileInputPlugin.class.getDeclaredMethod("listFiles", PluginTask.class);
        listFiles.setAccessible(true);
        task.setFiles((FileList) listFiles.invoke(plugin, task));

        assertArrayEquals(Files.readAllBytes(Paths.get(Resources.getResource("sample_out.csv").toURI())), Files.readAllBytes(out));

        final String last_path = runInputResult.getConfigDiff().getNested("in").get(String.class, "last_path");
        newConfig = embulk.newConfig().merge(config.set("last_path", last_path));
        task = CONFIG_MAPPER.map(newConfig, PluginTask.class);
        out = embulk.createTempFile("csv");
        embulk.runInput(newConfig, out);
        task.setFiles((FileList) listFiles.invoke(plugin, task));

        assertEquals(out.toFile().length(), 0);
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

    public ConfigSource config()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "azure_blob_storage")
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
    {
        FileList.Builder builder = new FileList.Builder(task);
        for (String file : fileList) {
            builder.add(file, 0);
        }
        return builder.build();
    }

    private void runInputAndExpect(final ConfigSource config, final String errorMsg) throws IOException
    {
        try {
            embulk.runInput(config, embulk.createTempFile("csv"));
        }
        catch (final PartialExecutionException ex) {
            assertTrue(ex.getCause() instanceof ConfigException);
            assertTrue(ex.getCause().getCause() instanceof JsonMappingException);
            assertTrue(ex.getCause().getCause().getMessage().startsWith(errorMsg));
            return;
        }
        fail("Expected Exception was not thrown.");
    }
}
