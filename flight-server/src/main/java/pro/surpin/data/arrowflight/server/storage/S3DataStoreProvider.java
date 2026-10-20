package pro.surpin.data.arrowflight.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;
import pro.surpin.data.arrowflight.spi.storage.StoreContext;
import pro.surpin.data.arrowflight.spi.storage.StoreUse;

/**
 * Opens S3 buckets ({@code s3://}, {@code s3a://}, {@code s3n://}) as stores.
 */
public final class S3DataStoreProvider implements DataStoreProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataStoreProvider.class);

    @Override
    public String name() {
        return "s3";
    }

    @Override
    public Set<String> schemes() {
        return Set.of("s3", "s3a", "s3n");
    }

    @Override
    public List<ConfigKey> configKeys() {
        return S3Settings.CONFIG_KEYS;
    }

    @Override
    public DataStore open(StoreContext context, String root, StoreUse use) {
        S3Settings settings = S3Settings.from(context.config(), context.ioParallelism(), true);
        settings.requireUsable();
        context.redactor().add(settings.secretKey());
        context.redactor().add(settings.sessionToken());
        String node = context.nodeName();
        List<String> ignored = S3Settings.awsEnvironmentVariables(System.getenv());
        if (!ignored.isEmpty()) {
            LOGGER.warn("node={} s3=awsEnvironmentIgnored variables={} (this server reads only"
                    + " FLIGHT_S3_* and its own settings)", node, ignored);
        }
        if (settings.useSsl() && !settings.verifyTls()) {
            LOGGER.warn("node={} s3=tlsNotVerified endpoint={} (s3VerifyTls=false: any certificate"
                    + " is accepted)", node, settings.endpointUri());
        }
        S3Client client = S3DataStore.createClient(settings);
        LOGGER.info("node={} s3=open dataDir={} endpoint={} region={} urlStyle={} tls={}"
                + " caFile={} listingTtlSeconds={} maxConnections={}", node,
                S3Uri.parse(root).uri(), settings.endpointUri(), settings.region(),
                settings.urlStyle(), settings.useSsl(), settings.caFile(),
                settings.listingTtlSeconds(), settings.maxConnections());
        return new S3DataStore(root, settings, client, System::nanoTime,
                context.threadFactory("s3-listing"), node);
    }
}
