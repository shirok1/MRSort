package c3g2.mrsort;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.*;

public class Sink {
    static class SinkArgs {
        @Parameter(names = {"-s", "--start"})
        public char start = 'a';

        @Parameter(names = {"-e", "--end"})
        public char end = 'z';

        @Parameter(names = {"-p", "--port"})
        public int port = 5555;

        @Parameter(names = {"--cache"})
        public Path cache = Paths.get("C:\\Users\\Shiroki\\Code\\MRSort\\sorted\\");

        @Parameter(names = {"--result"})
        public Path result = Paths.get("C:\\Users\\Shiroki\\Code\\MRSort\\sorted\\result");
    }

    private final static Logger LOG = LogManager.getLogger(Sink.class);

    private static class Combo {
        public final byte category;
        public final byte second;
        public final ConcurrentSkipListSet<PersistedFile> queue;
        public final AtomicLong counter;

        public Combo(byte category, byte second) {
            this.category = category;
            this.second = second;
            this.queue = new ConcurrentSkipListSet<>(Comparator.comparingInt(PersistedFile::getLevel));
            this.counter = new AtomicLong();
        }
    }

    public static void main(String[] args) {
        SinkArgs parameter = new SinkArgs();
        JCommander.newBuilder()
                .addObject(parameter)
                .build()
                .parse(args);

        assert parameter.start <= parameter.end;

        LOG.info("Sink range: {} to {}.", parameter.start, parameter.end);

        if (parameter.start == 'a' && parameter.end == 'z')
            LOG.warn("Running a full sink. This should not happen in production environment.");

        try (ZContext context = new ZContext()) {
            Combo[][] sinks = new Combo[parameter.end - parameter.start + 1][26];
            for (char i = parameter.start; i <= parameter.end; i++) {
                for (char j = 'a'; j <= 'z'; j++) {
                    LOG.debug("Creating sink for cat {} sec {}", i, j);
                    sinks[i - parameter.start][j - 'a'] = new Combo((byte) i, (byte) j);
                }
            }

            Socket socket = context.createSocket(SocketType.PULL);
            socket.bind("tcp://*:" + parameter.port);

            LOG.info("Ready for PUSH! Port: {}", parameter.port);

            ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 16,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());

            ByteBuffer buffer = ByteBuffer.allocate(Pusher.BUFSIZE);

            while (!Thread.currentThread().isInterrupted()) {
                int size = socket.recvByteBuffer(buffer, 0);
                if (size < 1) {
                    LOG.info("Received empty string!");
                    break;
                }
                buffer.flip();
                byte cat = buffer.get(0);
                byte sec = buffer.get(1);

                Combo combo = sinks[cat - parameter.start][sec - 'a'];
                PersistedFile persisted = PersistedFile.fromInput(buffer, combo.counter, parameter.cache);
                executor.execute(() ->
                        fileCreated(cat, sec, combo.queue, combo.counter, executor, parameter.cache)
                                .accept(persisted));
                buffer.clear();
            }

            socket.close();
            LOG.info("Waiting for thread pool to empty...");
            while (executor.getQueue().size() != 0 && executor.getActiveCount() != 0) {
                LOG.info("Still waiting...({} waiting, {} running)",
                        executor.getQueue().size(), executor.getActiveCount());
                //noinspection BusyWait
                Thread.sleep(500);
            }
            executor.shutdown();
            while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                LOG.info("Waiting for final termination...");
            }

            LOG.warn("Force merging files...");
            if (!Files.isDirectory(parameter.result)) {
                LOG.info("Result directory not existing, creating.");
                Files.createDirectory(parameter.result);
            }
            for (Combo[] catArray : sinks) {
                byte cat = catArray[0].category;
                byte sec = catArray[0].second;
                List<PersistedFile> result = Arrays.stream(catArray).map(c -> c.queue.parallelStream()
                        .reduce((a, b) -> PersistedFile.sortMerge
                                (c.category, c.second, a, b, c.counter, parameter.cache)))
                        .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
                LOG.info("String started with {} merged, now decompressing.", (char) cat);
                Path target = parameter.result.resolve("result" + (char) cat + ".txt");
                try (SeekableByteChannel outc = Files.newByteChannel(target, WRITE, CREATE, TRUNCATE_EXISTING)) {
                    for (PersistedFile file : result) file.decompress(outc, cat, sec);
                    LOG.info("Decompressed to {}.", target.getFileName());
                } catch (IOException e) {
                    LOG.error("Failed to decompress {}: {}", target.getFileName(), e);
                }
            }
            LOG.info("Merge finished.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Consumer<PersistedFile> fileCreated(byte cat, byte sec, Set<PersistedFile> q, AtomicLong counter, Executor ex, Path cacheDir) {
        return file -> {
            Optional<PersistedFile> best = q.stream().filter(b -> b.getLevel() == file.getLevel()).findFirst();
            if (!best.isPresent())
                best = q.stream().filter(levelInRange(file)).findFirst();
            if (!best.isPresent()) {
                q.add(file);
            } else {
                PersistedFile b = best.get();
                q.remove(b);
                CompletableFuture.supplyAsync(() -> PersistedFile.sortMerge(cat, sec, file, b, counter, cacheDir), ex)
                        .thenAcceptAsync(fileCreated(cat, sec, q, counter, ex, cacheDir));
            }
        };
    }

    private static Predicate<PersistedFile> levelInRange(PersistedFile file) {
        return b -> file.getLevel() - 1 <= b.getLevel() && b.getLevel() <= file.getLevel() + 1;
    }
}
