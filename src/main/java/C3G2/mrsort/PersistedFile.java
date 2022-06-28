package c3g2.mrsort;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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


    public static PersistedFile sortMergeMapped(byte category, byte second, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
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
            LOG.info("MERGED {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
        } catch (IOException e) {
            LOG.error("Failed to merge {} and {} into {} :{}", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName(), e);
        }

        System.gc();

        try {
            Files.delete(apf.path);
            Files.delete(bpf.path);
        } catch (IOException e) {
            LOG.warn("Failed to delete {} and {}: {}", apf.path.getFileName(), bpf.path.getFileName(), e);
        }
        return new PersistedFile(target, newLevel);
    }

    public static PersistedFile sortMergeBuffered(byte category, byte second, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
        int newLevel = Integer.max(apf.level, bpf.level) + 1;
        Path target = targetDir.resolve(String.format("%c%c_%d_%d.txt", category, second, newLevel, counter.getAndIncrement()));
        LOG.info("MERGING {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
        try (FileChannel outChannel = FileChannel.open(target, READ, WRITE, CREATE, TRUNCATE_EXISTING);
             FileChannel channelA = FileChannel.open(apf.path, READ);
             FileChannel channelB = FileChannel.open(bpf.path, READ)) {
            ByteBuffer bufferA = ByteBuffer.allocateDirect((int) Long.min(Integer.MAX_VALUE, channelA.size()));
            ByteBuffer bufferB = ByteBuffer.allocateDirect((int) Long.min(Integer.MAX_VALUE, channelB.size()));

            ByteBuffer outBuffer = ByteBuffer.allocateDirect((int) (channelA.size() + channelB.size()));

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

    public static PersistedFile sortMerge(byte category, byte second, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
        if (apf.path.toFile().length() + bpf.path.toFile().length() <= Integer.MAX_VALUE) {
            return sortMergeBuffered(category, second, apf, bpf, counter, targetDir);
        }

        LOG.warn("Using non-MappedByteBuffer for {} and {}", apf.path.getFileName(), bpf.path.getFileName());

        int newLevel = Integer.max(apf.level, bpf.level) + 1;
        Path target = targetDir.resolve(String.format("%c%c_%d_%d.txt", category, second, newLevel, counter.getAndIncrement()));
        LOG.info("MERGING {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
        try (FileOutputStream out = new FileOutputStream(target.toFile());
             FileInputStream fa = new FileInputStream(apf.path.toFile());
             FileInputStream fb = new FileInputStream(bpf.path.toFile())
                /*RandomAccessFile out = new RandomAccessFile(target.toFile(), "w");
             RandomAccessFile fa = new RandomAccessFile(apf.path.toFile(), "r");
             RandomAccessFile fb = new RandomAccessFile(bpf.path.toFile(), "r")*/) {
            ByteBuffer bufferA = ByteBuffer.allocate(8);
            ByteBuffer bufferB = ByteBuffer.allocate(8);

            FileChannel channelA = fa.getChannel();
            FileChannel channelB = fb.getChannel();
            FileChannel outChannel = out.getChannel();

            boolean statusA = channelA.read(bufferA) != -1;
            boolean statusB = channelB.read(bufferB) != -1;
            bufferA.flip();
            bufferB.flip();

            while (statusA && statusB) {
                int compare = Long.compareUnsigned(bufferA.getLong(), bufferB.getLong());
                bufferA.position(0);
                bufferB.position(0);
                if (compare < 0) {
                    outChannel.write(bufferA);
                    bufferA.clear();
                    statusA = channelA.read(bufferA) != -1;
                    bufferA.flip();
                } else {
                    outChannel.write(bufferB);
                    bufferB.clear();
                    statusB = channelB.read(bufferB) != -1;
                    bufferB.flip();
                }
            }
            channelA.transferTo(channelA.position(), channelA.size(), outChannel);
            channelB.transferTo(channelB.position(), channelB.size(), outChannel);

            channelA.close();
            channelB.close();
            outChannel.close();

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

    public void decompress(ByteChannel targetChannel, byte category, byte second) throws IOException {
        try (SeekableByteChannel inc = Files.newByteChannel(path, READ)) {
            int ioBufferSize = 1024 * 1024 * 32;
            ByteBuffer readBuffer = ByteBuffer.allocateDirect(ioBufferSize);
            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(ioBufferSize * 2);

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
