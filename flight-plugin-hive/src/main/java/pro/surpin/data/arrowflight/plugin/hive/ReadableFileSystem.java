package pro.surpin.data.arrowflight.plugin.hive;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * A read-only Hadoop file system with one file: a file of the server's stores, so that orc-core
 * reads it through the store (HDFS with Kerberos, S3, lakeFS) instead of a Hadoop client of its
 * own. Every path names that file; nothing can be listed or written.
 */
final class ReadableFileSystem extends FileSystem {

    private final ReadableFile file;
    private final Path path;

    /**
     * Wraps a file.
     *
     * @param file opened file
     * @param path the path orc-core is given for it
     */
    ReadableFileSystem(ReadableFile file, Path path) {
        this.file = file;
        this.path = path;
    }

    @Override
    public URI getUri() {
        return path.toUri();
    }

    @Override
    public String getScheme() {
        return "readable";
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) {
        return new FSDataInputStream(new Stream(file));
    }

    @Override
    public FileStatus getFileStatus(Path f) {
        return new FileStatus(file.length(), false, 1, file.length(), 0L, path);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException {
        return new FileStatus[] {getFileStatus(f)};
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
            int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException {
        throw new IOException("read-only");
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException {
        throw new IOException("read-only");
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new IOException("read-only");
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        throw new IOException("read-only");
    }

    @Override
    public void setWorkingDirectory(Path dir) {
    }

    @Override
    public Path getWorkingDirectory() {
        return path.getParent();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new IOException("read-only");
    }

    /** A seekable stream over positional reads of the file. */
    static final class Stream extends InputStream implements Seekable, PositionedReadable {

        private final ReadableFile file;
        private long position;

        /**
         * Opens a stream at the start.
         *
         * @param file file
         */
        Stream(ReadableFile file) {
            this.file = file;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = read(position, buffer, offset, length);
            if (read > 0) {
                position += read;
            }
            return read;
        }

        @Override
        public int read(long at, byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            long left = file.length() - at;
            if (left <= 0) {
                return -1;
            }
            int count = (int) Math.min(length, left);
            file.readFully(at, buffer, offset, count);
            return count;
        }

        @Override
        public void readFully(long at, byte[] buffer, int offset, int length) throws IOException {
            if (at < 0 || at + length > file.length()) {
                throw new EOFException("Range " + at + "+" + length + " beyond " + file.length());
            }
            file.readFully(at, buffer, offset, length);
        }

        @Override
        public void readFully(long at, byte[] buffer) throws IOException {
            readFully(at, buffer, 0, buffer.length);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos > file.length()) {
                throw new EOFException("Cannot seek to " + pos + " of " + file.length());
            }
            position = pos;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int available() {
            return (int) Math.min(Integer.MAX_VALUE, Math.max(0, file.length() - position));
        }
    }
}
