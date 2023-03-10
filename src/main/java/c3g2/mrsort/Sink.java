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
        public String startArg = "a";

        @Parameter(names = {"-e", "--end"})
        public String endArg = "z";

        public char start = 'a';
        public char end = 'z';

        @Parameter(names = {"-p", "--port"})
        public int port = 5555;

        @Parameter(names = {"--cache"})
        public Path cache = Paths.get("C:\\Users\\Shiroki\\Code\\MRSort\\sorted\\");

        @Parameter(names = {"--result"})
        public Path result = Paths.get("C:\\Users\\Shiroki\\Code\\MRSort\\sorted\\result");
    }

    private final static Logger LOG = LogManager.getLogger(Sink.class);

    private static class CatCombo {
        public final byte category;

        public final SecondCombo[] secondCombos;

        static class SecondCombo {
            public final byte second;
            public final ConcurrentSkipListSet<PersistedFile> queue;
            public final AtomicLong counter;

            SecondCombo(byte second) {
                this.second = second;
                this.queue = new ConcurrentSkipListSet<>(Comparator.comparingInt(PersistedFile::getLevel));
                this.counter = new AtomicLong(0);
            }
        }

        public CatCombo(byte category) {
            this.category = category;
            this.secondCombos = new SecondCombo[26];
            for (int i = 0; i < 26; i++) {
                this.secondCombos[i] = new SecondCombo((byte) (i + 'a'));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        SinkArgs parameter = new SinkArgs();
        JCommander.newBuilder()
                .addObject(parameter)
                .build()
                .parse(args);

        parameter.start = parameter.startArg.charAt(0);
        parameter.end = parameter.endArg.charAt(0);

        assert parameter.start <= parameter.end;

        LOG.info("Sink range: {} to {}.", parameter.start, parameter.end);

        if (parameter.start == 'a' && parameter.end == 'z')
            LOG.warn("Running a full sink. This should not happen in production environment.");

        if (!Files.isDirectory(parameter.cache)) {
            LOG.warn("Cache dir not exists, creating.");
            Files.createDirectory(parameter.cache);
        }

        try (ZContext context = new ZContext()) {
            // raise High Watermark to prevent overflow (maybe)
            context.setRcvHWM(1024 * 1024);

            CatCombo[] sinks = new CatCombo[parameter.end - parameter.start + 1];
            for (char i = parameter.start; i <= parameter.end; i++) {
                LOG.debug("Creating sink for cat {}", i);
                sinks[i - parameter.start] = new CatCombo((byte) i);
            }

            Socket socket = context.createSocket(SocketType.PULL);
            socket.bind("tcp://*:" + parameter.port);

            LOG.info("Ready for PUSH! Port: {}", parameter.port);

            // for file merge only
            ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4,
                    0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

            // for socket recv
            ByteBuffer buffer = ByteBuffer.allocate(Pusher.BUF_SIZE);

            int stopSignalCount = 8;

            while (!Thread.currentThread().isInterrupted()) {
                int size = socket.recvByteBuffer(buffer, 0);
                if (size < 1) {
                    stopSignalCount--;
                    LOG.info("Received empty string! {} more to shutdown.", stopSignalCount);
                    if (stopSignalCount != 0) {
                        buffer.clear();
                        continue;
                    }else{
                        LOG.info("Break!");
                        break;
                    }
                }
                buffer.flip();
                byte cat = buffer.get(0);
                byte sec = buffer.get(1);

                if (cat >= parameter.start - 'a' + 'A' && cat <= parameter.end - 'a' + 'A') {
                    // is not a full pack
                    cat = (byte) (cat + 'a' - 'A');
                    buffer.limit(buffer.getInt(Pusher.BUF_SIZE - 4));
                } else if (cat < parameter.start || cat > parameter.end) {
                    LOG.warn("Received invalid head: {}{}", (char) cat, (char) sec);
                    buffer.clear();
                    continue;
                }

                CatCombo catCombo = sinks[cat - parameter.start];
                CatCombo.SecondCombo secondCombo = catCombo.secondCombos[sec - 'a'];
                PersistedFile persisted = PersistedFile.fromInput(buffer, secondCombo.counter, parameter.cache);
                byte finalCat = cat;
                executor.execute(() ->
                        fileCreated(finalCat, sec, secondCombo.queue, secondCombo.counter, executor, parameter.cache)
                                .accept(persisted));
                LOG.info("Created file {}, executor {} waiting, {} running.",
                        persisted.getPath().getFileName(), executor.getQueue().size(), executor.getActiveCount());
                buffer.clear();
            }

            socket.close();

            Thread.sleep(1000);

            LOG.info("Waiting for merge to finish...");
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

            {
                int ioBufferSize = 1024 * 1024 * 32;
                ByteBuffer readBuffer = ByteBuffer.allocateDirect(ioBufferSize);
                ByteBuffer writeBuffer = ByteBuffer.allocateDirect(ioBufferSize * 2);
                for (CatCombo catCombo : sinks) {
                    byte cat = catCombo.category;
                    class FileTuple {
                        public final PersistedFile file;
                        public final byte second;

                        public FileTuple(PersistedFile file, byte second) {
                            this.file = file;
                            this.second = second;
                        }
                    }
                    List<FileTuple> result = Arrays.stream(catCombo.secondCombos).map(c -> c.queue.parallelStream()
                                    .reduce((a, b) -> PersistedFile.sortMerge
                                            (cat, c.second, a, b, c.counter, parameter.cache)).map(f -> new FileTuple(f, c.second)))
                            .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
                    LOG.info("String started with {} merged, now decompressing.", (char) cat);
                    Path target = parameter.result.resolve("result" + (char) cat + ".txt");
                    try (SeekableByteChannel outc = Files.newByteChannel(target, WRITE, CREATE, TRUNCATE_EXISTING)) {
                        for (FileTuple tpl : result)
                            tpl.file.decompress(outc, cat, tpl.second, readBuffer, writeBuffer);
                        LOG.info("Decompressed to {}.", target.getFileName());
                    } catch (IOException e) {
                        LOG.error("Failed to decompress {}: {}", target.getFileName(), e);
                    }
                }
            }
            LOG.info("Merge finished.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * File created event.
     * Check the set to see if there is another file with similar level, and merge them or add the new file to the queue.
     * <p>Priority: same level, file with level - 1, file with level + 1.<p>
     * @param cat      category
     * @param sec      second char
     * @param q        {@link Set} (actually a thread safe priority queue) to add and get {@link PersistedFile}
     * @param counter  pass through to {@link PersistedFile#sortMerge(byte, byte, PersistedFile, PersistedFile, AtomicLong, Path) PersistedFile.sortMerge}
     * @param ex       merge task executor
     * @param cacheDir pass through to ...
     * @return a consumer
     */
    private static Consumer<PersistedFile> fileCreated(byte cat, byte sec, Set<PersistedFile> q, AtomicLong counter, Executor ex, Path cacheDir) {
        return file -> {
            synchronized (q) {
                Optional<PersistedFile> best = q.stream().filter(sameLevel(file)).findFirst();
                if (!best.isPresent())
                    best = q.stream().filter(levelInRange(file)).findFirst();
                if (!best.isPresent()) {
                    q.add(file);
                    LOG.info("No best match, the queue is now [{}]",
                            q.stream().map(PersistedFile::getFileName).collect(Collectors.joining(", ")));
                } else {
                    PersistedFile b = best.get();
                    LOG.info("Best match for {} found: {}", file.getFileName(), b.getFileName());
                    q.remove(b);
                    CompletableFuture.supplyAsync(() -> PersistedFile.sortMerge(cat, sec, file, b, counter, cacheDir), ex)
                            .thenAcceptAsync(fileCreated(cat, sec, q, counter, ex, cacheDir));
                }
            }
        };
    }

    private static Predicate<PersistedFile> sameLevel(PersistedFile file) {
        return b -> b.getLevel() == file.getLevel();
    }

    private static Predicate<PersistedFile> levelInRange(PersistedFile file) {
        return b -> file.getLevel() - 1 <= b.getLevel() && b.getLevel() <= file.getLevel() + 1;
    }
}
