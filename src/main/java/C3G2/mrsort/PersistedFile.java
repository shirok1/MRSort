package c3g2.mrsort;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

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

    public static PersistedFile sortMerge(byte category, byte second, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
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
            ByteBuffer head = ByteBuffer.allocate(2);
            head.put(category);
            head.put(second);
            head.flip();

            ByteBuffer compressedRest = ByteBuffer.allocate(8);
            ByteBuffer rest = ByteBuffer.allocate(14);
            rest.put(13, (byte) '\n');

            while (true) {
                int read = inc.read(compressedRest);
                if (read == -1) break;

                compressedRest.flip();
                for (int resti = 0; resti < read / 8; resti++) {
                    long restCopy = compressedRest.getLong();
                    for (int i = 12; i >= 0; i--) {
                        rest.put(i, (byte) (restCopy % 26 + 'a'));
                        restCopy /= 26;
                    }
                    rest.position(14);
                    rest.flip();
                    targetChannel.write(head);
                    head.position(0);
                    targetChannel.write(rest);
                    rest.clear();
                }
                compressedRest.clear();
            }
        }
    }
}
