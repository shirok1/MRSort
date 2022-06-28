package c3g2.mrsort;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

public class PersistedFile {
    private final static Logger LOG = LogManager.getLogger(PersistedFile.class);
    private final Path path;
    private final int level;

    public PersistedFile(Path path, int level) {
        this.path = path;
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public Path getPath() {
        return path;
    }

    public String getFileName() {
        return path.getFileName().toString();
    }

    public static PersistedFile fromInput(ByteBuffer buffer, AtomicLong counter, Path targetDir) {
        byte category = buffer.get();
        byte second = buffer.get();
        long count = counter.getAndIncrement();
        Path target = targetDir.resolve(String.format("%c%c_0_%d.txt", category, second, count));
        try (SeekableByteChannel c = Files.newByteChannel(target, WRITE, CREATE, TRUNCATE_EXISTING)) {
            c.write(buffer.slice());
        } catch (IOException e) {
            LOG.error("Failed to persist {}: {}", target.getFileName(), e);
            throw new RuntimeException(e);
        }
//        LOG.info("Created {}.", target.getFileName());
        return new PersistedFile(target, 0);
    }


    private static PersistedFile sortMergeMapped(byte category, byte second, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
        int newLevel = Integer.max(apf.level, bpf.level) + 1;
        Path target = targetDir.resolve(String.format("%c%c_%d_%d.txt", category, second, newLevel, counter.getAndIncrement()));
        LOG.info("MERGING {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
        try (FileChannel outChannel = FileChannel.open(target, READ, WRITE, CREATE, TRUNCATE_EXISTING);
             FileChannel channelA = FileChannel.open(apf.path, READ);
             FileChannel channelB = FileChannel.open(bpf.path, READ)) {
            MappedByteBuffer bufferA = channelA.map(READ_ONLY, 0, channelA.size());
            bufferA.load();

            MappedByteBuffer bufferB = channelB.map(READ_ONLY, 0, channelB.size());
            bufferB.load();

            MappedByteBuffer outBuffer = outChannel.map(READ_WRITE, 0, channelA.size() + channelB.size());

            while (bufferA.hasRemaining() && bufferB.hasRemaining()) {
                outBuffer.putLong(bufferA.getLong(bufferA.position()) < bufferB.getLong(bufferB.position())
                        ? bufferA.getLong() : bufferB.getLong());
            }

            outBuffer.put(bufferA);
            outBuffer.put(bufferB);

            outBuffer.force();
//            Cleaner cleaner = ((DirectBuffer) bufferA).cleaner();
//            if (cleaner != null) {
//                cleaner.clean();
//            }
//            cleaner = ((DirectBuffer) bufferB).cleaner();
//            if (cleaner != null) {
//                cleaner.clean();
//            }
            closeDirectBuffer(bufferA);
            closeDirectBuffer(bufferB);
            closeDirectBuffer(outBuffer);
            LOG.info("MERGED {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
        } catch (IOException e) {
            LOG.error("Failed to merge {} and {} into {} :{}", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName(), e);
        }

//        System.gc();

        try {
            Files.delete(apf.path);
            Files.delete(bpf.path);
        } catch (IOException e) {
            LOG.warn("Failed to delete {} and {}: {}", apf.path.getFileName(), bpf.path.getFileName(), e);
        }
        return new PersistedFile(target, newLevel);
    }

    private static PersistedFile sortMergeBuffered(byte category, byte second, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
        int newLevel = Integer.max(apf.level, bpf.level) + 1;
        Path target = targetDir.resolve(String.valueOf((char) category) + (char) second + "_" + newLevel + "_" + counter.getAndIncrement() + ".txt");
        LOG.info("MERGING {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
        try (FileChannel outChannel = FileChannel.open(target, READ, WRITE, CREATE, TRUNCATE_EXISTING);
             FileChannel channelA = FileChannel.open(apf.path, READ);
             FileChannel channelB = FileChannel.open(bpf.path, READ)) {
            ByteBuffer bufferA = ByteBuffer.allocate((int) Long.min(Integer.MAX_VALUE, channelA.size()));
            ByteBuffer bufferB = ByteBuffer.allocate((int) Long.min(Integer.MAX_VALUE, channelB.size()));

            ByteBuffer outBuffer = ByteBuffer.allocate((int) Long.min(Integer.MAX_VALUE, channelA.size() + channelB.size()));

            boolean statusA = channelA.read(bufferA) != -1;
            boolean statusB = channelB.read(bufferB) != -1;
            bufferA.flip();
            bufferB.flip();

            while (statusA && statusB) {
                while (bufferA.hasRemaining() && bufferB.hasRemaining()) {
                    if (!outBuffer.hasRemaining()) {
                        outBuffer.flip();
                        outChannel.write(outBuffer);
                        outBuffer.clear();
                    }
                    outBuffer.putLong(bufferA.getLong(bufferA.position()) < bufferB.getLong(bufferB.position())
                            ? bufferA.getLong() : bufferB.getLong());

                }
                if (!bufferA.hasRemaining()) {
                    bufferA.clear();
                    statusA = channelA.read(bufferA) != -1;
                    bufferA.flip();
                }
                if (!bufferB.hasRemaining()) {
                    bufferB.clear();
                    statusB = channelB.read(bufferB) != -1;
                    bufferB.flip();
                }
            }

            outBuffer.flip();
            outChannel.write(outBuffer);
            outChannel.write(bufferA);
            outChannel.write(bufferB);

            LOG.info("MERGED {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
        } catch (IOException e) {
            LOG.error("Failed to merge {} and {} into {} :{}", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName(), e);
        }

        try {
            Files.delete(apf.path);
            Files.delete(bpf.path);
        } catch (IOException e) {
            LOG.warn("Failed to delete {} and {}: {}", apf.path.getFileName(), bpf.path.getFileName(), e);
        }
        return new PersistedFile(target, newLevel);
    }

    private static void closeDirectBuffer(Buffer buffer) {
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.setAccessible(true);
            cleanMethod.invoke(cleaner);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            LOG.warn("Failed to close buffer: {}", e.toString());
        }
    }

    public static PersistedFile sortMerge(byte category, byte second, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
        if (apf.path.toFile().length() + bpf.path.toFile().length() < Integer.MAX_VALUE) {
            return sortMergeMapped(category, second, apf, bpf, counter, targetDir);
        } else {
            return sortMergeBuffered(category, second, apf, bpf, counter, targetDir);
        }
    }

    public void decompress(ByteChannel targetChannel, byte category, byte second, ByteBuffer readBuffer, ByteBuffer writeBuffer) throws IOException {
        try (SeekableByteChannel inc = Files.newByteChannel(path, READ)) {
            while (inc.read(readBuffer) != -1) {
                readBuffer.flip();
                while (readBuffer.hasRemaining()) {
                    int offset = writeBuffer.position();
                    writeBuffer.put(category);
                    writeBuffer.put(second);
                    long restCopy = readBuffer.getLong();
                    for (int i = 14; i >= 2; i--) {
                        writeBuffer.put(offset + i, (byte) (restCopy % 26 + 'a'));
                        restCopy /= 26;
                    }
                    writeBuffer.position(offset + 15);
                    writeBuffer.put((byte) '\n');
                }
                writeBuffer.flip();
                targetChannel.write(writeBuffer);

                readBuffer.clear();
                writeBuffer.clear();
            }
        }
        LOG.info("{} is decompressed.", path.getFileName());
    }
}
