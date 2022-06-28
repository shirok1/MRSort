package c3g2.mrsort;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.file.StandardOpenOption.READ;

public class Pusher {

    private final static Logger LOG = LogManager.getLogger(Pusher.class);

    public static final int CAP = 1024 * 1024;
    public static final int BUF_SIZE = 2 + 8 * CAP;

    static class PushChunk {
        public int getSize() {
            return size;
        }

        public long[] getData() {
            return chunk;
        }

        private long[] chunk;
        private int size;

        public PushChunk(int cap) {
            chunk = new long[cap];
            size = 0;
        }

        public long[] getAndReset(int capacity) {
            long[] result = chunk;
            chunk = new long[capacity];
            size = 0;
            return result;
        }

        public void add(long value) {
            chunk[size] = value;
            size++;
        }
    }


    static class Dispatcher implements AutoCloseable {
        private final ZContext context;
        private final List<ZMQ.Socket> sockets;

        private final boolean isLocal;

        Dispatcher(Stream<String> ips) {
            context = new ZContext(8);
            context.setSndHWM(1024 * 1024);

            sockets = ips.map(ip -> {
                ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
                socket.connect("tcp://" + ip + ":5555");
                return socket;
            }).collect(Collectors.toList());

            if (sockets.size() == 8) {
                isLocal = false;
                LOG.info("Connected to 8 Sinks.");
            } else if (sockets.size() == 1) {
                isLocal = true;
                LOG.info("Localhost mode.");
            } else {
                throw new IllegalArgumentException("Wrong number of IPs given.");
            }
        }

        public ZMQ.Socket getSocketForCat(byte cat) {
            if (isLocal) {
                return sockets.get(0);
            } else {
                switch (cat) {
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                        return sockets.get(0);
                    case 'e':
                    case 'f':
                    case 'g':
                    case 'h':
                        return sockets.get(1);
                    case 'i':
                    case 'j':
                    case 'k':
                        return sockets.get(2);
                    case 'l':
                    case 'm':
                    case 'n':
                        return sockets.get(3);
                    case 'o':
                    case 'p':
                    case 'q':
                        return sockets.get(4);
                    case 'r':
                    case 's':
                    case 't':
                        return sockets.get(5);
                    case 'u':
                    case 'v':
                    case 'w':
                        return sockets.get(6);
                    case 'x':
                    case 'y':
                    case 'z':
                        return sockets.get(7);
                    default:
                        throw new IllegalArgumentException("Invalid category: " + cat);
                }
            }
        }

        @Override
        public void close() {
            sockets.forEach(ZMQ.Socket::close);
            context.close();
        }

        public static Dispatcher local() {
            return new Dispatcher(Stream.of("localhost"));
        }

        public void foreach(Consumer<ZMQ.Socket> consumer) {
            sockets.forEach(consumer);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length != 0 && args.length != 1 && args.length != 9) {
            LOG.error("Don't know what to do!");
            System.exit(1);
        }

        Path fromPath = Paths.get(Arrays.stream(args).skip(args.length - 1).findFirst().orElse("C:\\Users\\Shiroki\\Code\\MRSort\\data01.txt"));
        List<Path> files = (Files.isDirectory(fromPath)
                ? StreamSupport.stream(Files.newDirectoryStream(fromPath).spliterator(), false)
                .filter(p -> p.getFileName().toString().matches("data0\\d.txt"))
                : Stream.of(fromPath)).collect(Collectors.toList());
        LOG.info("Selecting files: {}", files.stream().map(f -> f.getFileName().toString()).collect(Collectors.joining(", ")));

        PushChunk[][] chunks = new PushChunk[26][26];
        for (int i = 0; i < 26; i++) {
            for (int j = 0; j < 26; j++) {
                chunks[i][j] = new PushChunk(CAP);
            }
        }
        Runtime runtime = Runtime.getRuntime();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(6, 6,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        try (Dispatcher disp = args.length != 9
                ? Dispatcher.local()
                : new Dispatcher(Arrays.stream(args).limit(8).map(String::trim))) {
            ByteBuffer buffer = ByteBuffer.allocate(512 * 1024 * 1024);
            for (Path file : files) {
                try (SeekableByteChannel fc = Files.newByteChannel(file, READ)) {
                    long fileSize = fc.size();
                    long startTime = System.currentTimeMillis();
                    long lastTime = startTime;
                    long lastPosition = fc.position();
                    String fileName = file.getFileName().toString();
                    while (true) {
                        int read = fc.read(buffer);
                        if (read == -1) break;

                        buffer.flip();

                        {
                            long now = System.currentTimeMillis();
                            long filePosition = fc.position();
                            long speed = (filePosition - lastPosition) / (now - lastTime + 1); // b/ms == kb/s
                            long remain = (fileSize - filePosition) / (speed + 1) / 1024;
                            LOG.info(String.format("[Sending] File: %s(%,dMb) Position: %,dMb Speed: %,dKb/s Remain: %d:%02d",
                                    fileName, fileSize / (1024 * 1024), fc.position() / (1024 * 1024),
                                    speed, remain / 60, remain % 60));
                            lastTime = now;
                            lastPosition = filePosition;
                            LOG.info(String.format("[Memory] Used: %,dMb Free: %,dMb Total: %,dMb",
                                    (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024),
                                    runtime.freeMemory() / (1024 * 1024), runtime.totalMemory() / (1024 * 1024)));
                        }

                        for (int offset = 0; offset < read; offset += 16) {
                            byte cat = buffer.get(offset);
                            byte second = buffer.get(offset + 1);
                            long rest = 0L;
                            for (int i = 2; i < 15; i++) {
                                rest = rest * 26 + buffer.get(offset + i) - 'a';
                            }
                            PushChunk chunk = chunks[cat - 'a'][second - 'a'];
                            chunk.add(rest);
                            if (chunk.getSize() == CAP) {
                                int size = chunk.getSize();
                                long[] data = chunk.getAndReset(CAP);
                                executor.execute(() -> {
                                    ByteBuffer sendBuffer = ByteBuffer.allocate(BUF_SIZE);
                                    Arrays.sort(data);
                                    sendBuffer.put(cat);
                                    sendBuffer.put(second);
                                    for (int i = 0; i < size; i++) {
                                        sendBuffer.putLong(data[i]);
                                    }
                                    sendBuffer.flip();
                                    disp.getSocketForCat(cat).sendByteBuffer(sendBuffer, 0);
                                });
                            }
                        }
                        buffer.clear();
                    }
                    long timeElapsed = System.currentTimeMillis() - startTime;
                    LOG.info(String.format("[Sending] File: %s(%,dMb) finished. Time: %d:%02d Avg Speed: %,dKb/s",
                            fileName, fileSize / (1024 * 1024),
                            timeElapsed / 1024 / 60, timeElapsed / 1024 % 60, fileSize / timeElapsed));
                    System.gc();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            ByteBuffer sendBuffer = ByteBuffer.allocate(BUF_SIZE);
            for (int i = 0; i < 26; i++) {
                for (int j = 0; j < 26; j++) {
                    PushChunk chunk = chunks[i][j];
                    int size = chunk.getSize();
                    long[] data = chunk.getData();
                    byte cat = (byte) (i + 'a');
                    byte second = (byte) (j + 'a');
                    Arrays.sort(data, 0, size);
                    sendBuffer.put(cat);
                    sendBuffer.put(second);
                    for (int count = 0; count < size; count++) {
                        sendBuffer.putLong(data[count]);
                    }
                    sendBuffer.flip();
                    disp.getSocketForCat(cat).sendByteBuffer(sendBuffer, 0);
                    sendBuffer.clear();
                }
            }

            LOG.info("Waiting for thread pool to empty...");
            while (executor.getQueue().size() != 0 && executor.getActiveCount() != 0) {
                LOG.info("Still waiting...({} waiting, {} running)",
                        executor.getQueue().size(), executor.getActiveCount());
                //noinspection BusyWait
                Thread.sleep(500);
            }
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("Waiting for executor to terminate");
            }

            disp.foreach(socket -> socket.send("", 0));
            LOG.info("All done.");
        }
    }
}
