package pro.surpin.data.arrowflight.server.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A plugin directory as its descriptor {@code META-INF/arrowflight/plugin.properties} describes
 * it, read from the jars as files: no class of the plugin is loaded to read it.
 *
 * @param name plugin name, equal to its directory name
 * @param spi SPI version it was built against, {@code major.minor}
 * @param storageSchemes URI schemes its storage providers open
 * @param catalogs names of its catalog providers
 * @param shareHadoop whether it uses the server's Hadoop classes ({@code share=hadoop})
 * @param directory plugin directory
 * @param jars jars of the directory, in name order
 */
public record PluginDescriptor(String name, String spi, Set<String> storageSchemes,
        Set<String> catalogs, boolean shareHadoop, Path directory, List<Path> jars) {

    /** Where a plugin jar carries its descriptor. */
    public static final String DESCRIPTOR = "META-INF/arrowflight/plugin.properties";

    /**
     * Copies the collections.
     *
     * @param name plugin name
     * @param spi SPI version
     * @param storageSchemes storage schemes
     * @param catalogs catalog names
     * @param shareHadoop whether Hadoop is shared
     * @param directory plugin directory
     * @param jars jars
     */
    public PluginDescriptor {
        storageSchemes = Set.copyOf(storageSchemes);
        catalogs = Set.copyOf(catalogs);
        jars = List.copyOf(jars);
    }

    /**
     * Reads the descriptor of a plugin directory.
     *
     * @param directory plugin directory, {@code plugins/<name>}
     * @return descriptor
     * @throws IllegalArgumentException when no jar or more than one carries a descriptor, or the
     *         descriptor is incomplete
     */
    public static PluginDescriptor read(Path directory) {
        List<Path> jars = jars(directory);
        Properties descriptor = null;
        Path carrier = null;
        for (Path jar : jars) {
            try (ZipFile zip = new ZipFile(jar.toFile())) {
                ZipEntry entry = zip.getEntry(DESCRIPTOR);
                if (entry == null) {
                    continue;
                }
                if (descriptor != null) {
                    throw new IllegalArgumentException("Plugin directory " + directory + " has two"
                            + " descriptors: " + carrier.getFileName() + " and " + jar.getFileName());
                }
                descriptor = new Properties();
                try (InputStream input = zip.getInputStream(entry)) {
                    descriptor.load(input);
                }
                carrier = jar;
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read plugin jar " + jar, e);
            }
        }
        if (descriptor == null) {
            throw new IllegalArgumentException("Plugin directory " + directory + " has no jar with "
                    + DESCRIPTOR);
        }
        String name = descriptor.getProperty("name", "").trim();
        Path fileName = directory.getFileName();
        if (name.isEmpty() || fileName == null || !name.equals(fileName.toString())) {
            throw new IllegalArgumentException("Plugin descriptor in " + directory + " names \""
                    + name + "\"; the directory must have the plugin's name");
        }
        String spi = descriptor.getProperty("spi", "").trim();
        String share = descriptor.getProperty("share", "").trim().toLowerCase(Locale.ROOT);
        if (!share.isEmpty() && !"hadoop".equals(share)) {
            throw new IllegalArgumentException("Plugin " + name + ": share must be empty or hadoop: "
                    + share);
        }
        return new PluginDescriptor(name, spi, list(descriptor.getProperty("provides.storage")),
                list(descriptor.getProperty("provides.catalog")), "hadoop".equals(share), directory,
                jars);
    }

    /**
     * Lists the jars of a directory.
     *
     * @param directory plugin directory
     * @return jars in name order
     */
    static List<Path> jars(Path directory) {
        try (Stream<Path> files = Files.list(directory)) {
            List<Path> result = new ArrayList<>(files
                    .filter(p -> p.getFileName() != null && p.getFileName().toString().endsWith(".jar"))
                    .filter(Files::isRegularFile).sorted().toList());
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list plugin directory " + directory, e);
        }
    }

    /**
     * Splits a comma separated list.
     *
     * @param value list or null
     * @return lower case items
     */
    private static Set<String> list(String value) {
        Set<String> result = new LinkedHashSet<>();
        if (value != null) {
            Arrays.stream(value.split(",")).map(String::trim).filter(v -> !v.isEmpty())
                    .map(v -> v.toLowerCase(Locale.ROOT)).forEach(result::add);
        }
        return result;
    }
}
