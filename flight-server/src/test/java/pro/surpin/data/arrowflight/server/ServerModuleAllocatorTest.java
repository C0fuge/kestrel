package pro.surpin.data.arrowflight.server;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.OutboundBudget;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies server Arrow allocations respect the configured process budget. */
@Tag("unit")
class ServerModuleAllocatorTest {

    /** Checks that the module passes the configured limit to the root allocator. */
    @Test
    void allocatorUsesConfiguredLimit() {
        ServerModule module = new ServerModule(new String[0], "grpc+tcp://localhost:32010",
                "localhost", new Configuration());
        String previous = System.getProperty("arrowAllocatorLimitBytes");
        System.setProperty("arrowAllocatorLimitBytes", "1073741824");
        try (BufferAllocator allocator = module.allocator(ConfigAdapter.getConfig())) {
            assertEquals(1_073_741_824L, allocator.getLimit());
        } finally {
            if (previous == null) {
                System.clearProperty("arrowAllocatorLimitBytes");
            } else {
                System.setProperty("arrowAllocatorLimitBytes", previous);
            }
        }
    }

    /** Q3: without a setting the root allocator gets 75 % of direct memory, at least 256 MiB. */
    @Test
    void allocatorLimitFollowsDirectMemoryByDefault() {
        ServerModule module = new ServerModule(new String[0], "grpc+tcp://localhost:32010",
                "localhost", new Configuration());
        long expected = ConfigAdapter.defaultArrowAllocatorLimitBytes(OutboundBudget.maxDirectMemory());
        try (BufferAllocator allocator = module.allocator(ConfigAdapter.getConfig())) {
            assertEquals(expected, allocator.getLimit());
        }
    }
}
