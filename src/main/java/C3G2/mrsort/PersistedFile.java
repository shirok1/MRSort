package c3g2.mrsort;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static java.nio.file.StandardOpenOption.*;

public class PersistedFile {
    private final static Logger LOG = LogManager.getLogger(PersistedFile.class);
    private final Path path;
    private final int level;

    public PersistedFile(Path path, int level) {
        this.path = path;
        this.level = level;
    }

    static Supplier<PersistedFile> persist(AtomicLong counter, List<CompressedString> list, Path targetDir) {
        return () -> {
            list.sort(null);
            long count = counter.getAndIncrement();
            byte category = list.get(0).getCategory();
            Path target = targetDir.resolve(String.format("%c_0_%d.txt", category, count));
            try (SeekableByteChannel c = Files.newByteChannel(target, WRITE, CREATE, TRUNCATE_EXISTING)) {
                ByteBuffer cache = ByteBuffer.allocate(10);
                for (CompressedString s : list) s.writeCompressedTo(c, cache);
            } catch (IOException e) {
                LOG.error("Failed to persist {}: {}", target.getFileName(), e);
                throw new RuntimeException(e);
            }
            LOG.info("Created {}.", target.getFileName());
            return new PersistedFile(target, 0);
        };
    }

    public int getLevel() {
        return level;
    }

    public Path getPath() {
        return path;
    }

    public static PersistedFile merge(byte category, PersistedFile apf, PersistedFile bpf, AtomicLong counter, Path targetDir) {
        int newLevel = Integer.max(apf.level, bpf.level) + 1;
        Path target = targetDir.resolve(String.format("%c_%d_%d.txt", category, newLevel, counter.getAndIncrement()));
        try (FileOutputStream out = new FileOutputStream(target.toFile());
             FileInputStream fa = new FileInputStream(apf.path.toFile());
             FileInputStream fb = new FileInputStream(bpf.path.toFile())
                /*RandomAccessFile out = new RandomAccessFile(target.toFile(), "w");
             RandomAccessFile fa = new RandomAccessFile(apf.path.toFile(), "r");
             RandomAccessFile fb = new RandomAccessFile(bpf.path.toFile(), "r")*/) {
            ByteBuffer bufferA = ByteBuffer.allocate(10);
            ByteBuffer bufferB = ByteBuffer.allocate(10);

            FileChannel channelA = fa.getChannel();
            FileChannel channelB = fb.getChannel();
            FileChannel outChannel = out.getChannel();

            boolean statusA = channelA.read(bufferA) != -1;
            boolean statusB = channelB.read(bufferB) != -1;
            bufferA.flip();
            bufferB.flip();

            while (statusA && statusB) {
                if (CompressedString.compare(bufferA, bufferB) < 0) {
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

            LOG.info("Merged {} and {} into {}.", apf.path.getFileName(), bpf.path.getFileName(), target.getFileName());
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

    public void decompress(Path target) throws IOException {
        try (SeekableByteChannel outc = Files.newByteChannel(target, WRITE, CREATE, TRUNCATE_EXISTING);
             SeekableByteChannel inc = Files.newByteChannel(path, READ)) {
            ByteBuffer head = ByteBuffer.allocate(2);
            ByteBuffer compressedRest = ByteBuffer.allocate(8);
            ByteBuffer rest = ByteBuffer.allocate(14);
            rest.put(13, (byte) '\n');

            while (inc.read(head) != -1 && inc.read(compressedRest) != -1) {
                head.flip();
                compressedRest.flip();
                long restCopy = compressedRest.asLongBuffer().get();
                for (int i = 12; i >= 0; i--) {
                    rest.put(i, (byte) (restCopy % 26 + 'a'));
                    restCopy /= 26;
                }
                rest.position(14);
                rest.flip();
                outc.write(head);
                outc.write(rest);
                head.clear();
                compressedRest.clear();
                rest.clear();
            }
        }
    }
}
