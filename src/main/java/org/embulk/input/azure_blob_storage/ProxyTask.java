package org.embulk.input.azure_blob_storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.microsoft.azure.storage.OperationContext;
import org.embulk.config.ConfigException;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.commons.lang3.StringUtils.upperCase;

public interface ProxyTask
        extends Task
{
    @Config("type")
    ProxyType getType();

    @Config("host")
    @ConfigDefault("null")
    Optional<String> getHost();

    @Config("port")
    @ConfigDefault("8080")
    int getPort();

    @Config("user")
    @ConfigDefault("null")
    Optional<String> getUser();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();

    enum ProxyType
    {
        HTTP;

        @JsonCreator
        public static ProxyType of(String value)
        {
            try {
                return ProxyType.valueOf(upperCase(value));
            }
            catch (IllegalArgumentException e) {
                throw new ConfigException("Unsupported 'proxy type': " + value + ", supported values: " + Arrays.toString(ProxyTask.ProxyType.values()));
            }
        }
    }

    static OperationContext toOperationContext(ProxyTask proxyTask)
    {
        if (proxyTask == null) {
            return null;
        }
        OperationContext op = new OperationContext();
        //current support only http proxy
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyTask.getHost().get().trim(), proxyTask.getPort()));
        op.setProxy(proxy);
        op.setProxyUsername(proxyTask.getUser().orElse(null));
        op.setProxyPassword(proxyTask.getPassword().orElse(null));
        op.setLoggingEnabled(true);
        return op;
    }
}
