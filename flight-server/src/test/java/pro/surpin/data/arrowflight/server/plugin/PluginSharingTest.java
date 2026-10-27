package pro.surpin.data.arrowflight.server.plugin;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.vector.IntVector;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@code share} descriptor value: which server libraries ({@code hadoop}, {@code arrow})
 * a plugin's class loader takes from the server, and what such a plugin may not ship itself.
 */
@Tag("unit")
class PluginSharingTest {

    @TempDir
    Path dir;

    @Test
    void sharedArrowClassesAreTheServers() throws Exception {
        ClassLoader server = getClass().getClassLoader();
        try (PluginClassLoader loader = new PluginClassLoader("t", new URL[0], server, false,
                true)) {
            assertSame(IntVector.class, loader.loadClass(IntVector.class.getName()));
            assertSame(ArrowArrayStream.class, loader.loadClass(ArrowArrayStream.class.getName()));
            assertThrows(ClassNotFoundException.class,
                    () -> loader.loadClass(Configuration.class.getName()));
        }
        try (PluginClassLoader loader = new PluginClassLoader("t", new URL[0], server, true,
                false)) {
            assertSame(Configuration.class, loader.loadClass(Configuration.class.getName()));
            assertThrows(ClassNotFoundException.class,
                    () -> loader.loadClass(IntVector.class.getName()));
            assertFalse(loader.isShared("org.apache.hadoop.hive.ql.exec.vector.ColumnVector"));
        }
    }

    @Test
    void shareListsHadoopAndArrow() throws Exception {
        PluginDescriptor both = PluginDescriptor.read(plugin("p", "share=hadoop, Arrow\n"));
        assertTrue(both.shareHadoop());
        assertTrue(both.shareArrow());
        PluginDescriptor arrow = PluginDescriptor.read(plugin("q", "share=arrow\n"));
        assertFalse(arrow.shareHadoop());
        assertTrue(arrow.shareArrow());
        PluginDescriptor none = PluginDescriptor.read(plugin("r", "share=\n"));
        assertFalse(none.shareHadoop());
        assertFalse(none.shareArrow());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> PluginDescriptor.read(plugin("s", "share=hadoop,netty\n")));
        assertTrue(e.getMessage().contains("netty"), e.getMessage());
    }

    @Test
    void aPluginSharingArrowMayNotShipArrow() throws Exception {
        Path shared = plugin("a", "share=arrow\n");
        jar(shared.resolve("arrow-vector.jar"), "org/apache/arrow/vector/IntVector.class");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> PluginManager.checkJars(PluginDescriptor.read(shared)));
        assertTrue(e.getMessage().contains("shares Arrow"), e.getMessage());

        Path own = plugin("b", "share=\n");
        jar(own.resolve("arrow-vector.jar"), "org/apache/arrow/vector/IntVector.class");
        PluginManager.checkJars(PluginDescriptor.read(own));
        assertEquals(2, PluginDescriptor.read(own).jars().size());
    }

    /** Lays out a plugin directory whose one jar carries a descriptor with the given extra lines. */
    private Path plugin(String name, String extra) throws IOException {
        Path directory = Files.createDirectories(dir.resolve(name));
        Path jar = directory.resolve(name + ".jar");
        try (OutputStream raw = Files.newOutputStream(jar);
                ZipOutputStream out = new ZipOutputStream(raw)) {
            out.putNextEntry(new ZipEntry(PluginDescriptor.DESCRIPTOR));
            out.write(("name=" + name + "\nspi=1.3\n" + extra).getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
        return directory;
    }

    /** Writes a jar with one empty entry. */
    private static void jar(Path file, String entry) throws IOException {
        try (OutputStream raw = Files.newOutputStream(file);
                ZipOutputStream out = new ZipOutputStream(raw)) {
            out.putNextEntry(new ZipEntry(entry));
            out.closeEntry();
        }
    }
}
