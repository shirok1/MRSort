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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Sink {
    static class SinkArgs {
        @Parameter(names = {"-s", "--start"})
        public char start = 'a';

        @Parameter(names = {"-e", "--end"})
        public char end = 'z';

        @Parameter(names = {"-p", "--port"})
        public int port = 5555;

        @Parameter(names = {"--capacity"})
        public int capacity = Integer.MAX_VALUE / 128;
//        public int capacity = 1024;

        @Parameter(names = {"--cache"})
        public Path cache = Paths.get("C:\\Users\\Shiroki\\Code\\MRSort\\sorted\\");

        @Parameter(names = {"--result"})
        public Path result = Paths.get("C:\\Users\\Shiroki\\Code\\MRSort\\sorted\\result");
    }

    private final static Logger LOG = LogManager.getLogger(Sink.class);

    private static class Combo {
        public final byte category;
        public ArrayList<CompressedString> sink;
        public final ConcurrentSkipListSet<PersistedFile> queue;
        public final AtomicLong counter;

        public Combo(byte category, int cap) {
            this.category = category;
            this.sink = new ArrayList<>(cap);
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
            Combo[] sinks = new Combo[parameter.end - parameter.start + 1];
            for (char i = parameter.start; i <= parameter.end; i++) {
                LOG.debug("Creating sink for topic {}", i);
                sinks[i - parameter.start] = new Combo((byte) i, parameter.capacity);
            }

            Socket socket = context.createSocket(SocketType.PULL);
            socket.bind("tcp://*:" + parameter.port);

            LOG.info("Ready for PUSH! Port: {}", parameter.port);

            ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 16,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());

            ByteBuffer buffer = ByteBuffer.allocate(15);

            while (!Thread.currentThread().isInterrupted()) {
                if (socket.recvByteBuffer(buffer, 0) < 1) {
                    LOG.info("Received empty string!");
                    break;
                }
                buffer.flip();
                CompressedString compressed = CompressedString.fromStringBuffer(buffer);
                buffer.clear();

                int catIndex = compressed.getCategoryIndex();
                Combo combo = sinks[catIndex];
                combo.sink.add(compressed);
                if (combo.sink.size() == parameter.capacity) {
                    ArrayList<CompressedString> oldSink = combo.sink;
                    combo.sink = new ArrayList<>(parameter.capacity);
                    CompletableFuture.supplyAsync(PersistedFile.persist(combo.counter, oldSink, parameter.cache), executor)
                            .thenAcceptAsync(fileCreated(compressed.getCategory(), combo.queue, combo.counter, executor, parameter.cache));
                }
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

            ExecutorService tmpExecutor = Executors.newSingleThreadExecutor();
            LOG.info("Cleaning unwritten strings...");
            Arrays.stream(sinks).filter(c -> !c.sink.isEmpty())
                    .map(c -> CompletableFuture.supplyAsync(PersistedFile.persist(c.counter, c.sink, parameter.cache), tmpExecutor)
                            .thenAcceptAsync(fileCreated(c.category, c.queue, c.counter, tmpExecutor, parameter.cache)))
                    .collect(Collectors.toSet()).forEach(CompletableFuture::join);
            tmpExecutor.shutdown();

            LOG.warn("Force merging files...");
            if (!Files.isDirectory(parameter.result)) {
                LOG.info("Result directory not existing, creating.");
                Files.createDirectory(parameter.result);
            }
            Arrays.stream(sinks).forEach(c -> c.queue.stream().reduce((a, b) -> PersistedFile.merge(c.category, a, b, c.counter, parameter.cache))
                    .ifPresent(last -> {
                        LOG.info("String started with {} merged into {}, now decompressing.", (char) c.category, last.getPath().getFileName());
                        Path target = parameter.result.resolve("result" + (char) c.category + ".txt");
                        try {
                            last.decompress(target);
                            LOG.info("Decompressed to {}.", target.getFileName());
                        } catch (IOException e) {
                            LOG.error("Failed to decompress {}: {}", target.getFileName(), e);
                        }
                    }));
            LOG.info("Merge finished.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Consumer<PersistedFile> fileCreated(byte cat, Set<PersistedFile> q, AtomicLong counter, Executor ex, Path cacheDir) {
        return file -> {
            Optional<PersistedFile> best = q.stream().filter(b -> b.getLevel() == file.getLevel()).findFirst();
            if (!best.isPresent())
                best = q.stream().filter(levelInRange(file)).findFirst();
            if (!best.isPresent()) {
                q.add(file);
            } else {
                PersistedFile b = best.get();
                q.remove(b);
                CompletableFuture.supplyAsync(() -> PersistedFile.merge(cat, file, b, counter, cacheDir), ex)
                        .thenAcceptAsync(fileCreated(cat, q, counter, ex, cacheDir));
            }
        };
    }

    private static Predicate<PersistedFile> levelInRange(PersistedFile file) {
        return b -> file.getLevel() - 1 <= b.getLevel() && b.getLevel() <= file.getLevel() + 1;
    }
}
