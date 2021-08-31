package org.embulk.input.azure_blob_storage;

import com.microsoft.azure.storage.OperationContext;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.junit.Rule;
import org.junit.Test;

import java.net.Proxy;

import static org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin.CONFIG_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class TestProxyTask
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testToOperationContext()
    {
        assertNull(ProxyTask.toOperationContext(null));

        ConfigSource configSource = runtime.getExec().newConfigSource()
                .set("type", "http")
                .set("host", "test_host")
                .set("port", 1234)
                .set("user", "test_user")
                .set("password", "test_pass");
        OperationContext operationContext = ProxyTask.toOperationContext(CONFIG_MAPPER.map(configSource, ProxyTask.class));

        assertNotNull(operationContext);
        assertEquals(operationContext.getProxy().type(), Proxy.Type.HTTP);
        assertEquals(operationContext.getProxy().address().toString(), "test_host:1234");
        assertEquals(operationContext.getProxyUsername(), "test_user");
        assertEquals(operationContext.getProxyPassword(), "test_pass");
    }

    @Test
    public void testProxyType()
    {
        assertEquals(ProxyTask.ProxyType.of("http"), ProxyTask.ProxyType.HTTP);
        ConfigException configException = assertThrows(ConfigException.class, () -> ProxyTask.ProxyType.of("invalid"));
        assertEquals(configException.getMessage(), "Unsupported 'proxy type': invalid, supported values: [HTTP]");
    }
}
