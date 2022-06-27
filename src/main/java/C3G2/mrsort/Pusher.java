package c3g2.mrsort;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.file.StandardOpenOption.READ;

public class Pusher {

    public static final int CAP = Integer.MAX_VALUE / 4096 / 32;
    public static final int BUFSIZE = 2 + 8 * CAP;

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

    public static void main(String[] args) throws InterruptedException, IOException {
        Path fromPath = Paths.get(Arrays.stream(args).findFirst().orElse("C:\\Users\\Shiroki\\Code\\MRSort\\data01.txt"));
        List<Path> files = (Files.isDirectory(fromPath)
                ? StreamSupport.stream(Files.newDirectoryStream(fromPath).spliterator(), false)
                .filter(p -> p.getFileName().toString().matches("data0\\d.txt"))
                : Stream.of(fromPath)).collect(Collectors.toList());
        PushChunk[][] chunks = new PushChunk[26][26];
        for (int i = 0; i < 26; i++) {
            for (int j = 0; j < 26; j++) {
                chunks[i][j] = new PushChunk(CAP);
            }
        }
        ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        try (ZContext context = new ZContext(8)) {
            org.zeromq.ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
            socket.connect("tcp://localhost:5555");
            ByteBuffer buffer = ByteBuffer.allocate(16);
            for (Path file : files) {
                try (SeekableByteChannel fc = Files.newByteChannel(file, READ)) {
                    while (fc.read(buffer) != -1) {
                        buffer.flip();
                        byte cat = buffer.get(0);
                        byte second = buffer.get(1);
                        long rest = 0L;
                        for (int i = 2; i < 15; i++) {
                            rest = rest * 26 + buffer.get(i) - 'a';
                        }
                        buffer.clear();
                        PushChunk chunk = chunks[cat - 'a'][second - 'a'];
                        chunk.add(rest);
                        if (chunk.getSize() == CAP) {
                            int size = chunk.getSize();
                            long[] data = chunk.getAndReset(CAP);
                            executor.execute(() -> {
                                ByteBuffer sendBuffer = ByteBuffer.allocate(BUFSIZE);
                                Arrays.sort(data);
                                sendBuffer.put(cat);
                                sendBuffer.put(second);
                                for (int i = 0; i < size; i++) {
                                    sendBuffer.putLong(data[i]);
                                }
                                sendBuffer.flip();
                                socket.sendByteBuffer(sendBuffer, 0);
                            });
                        }

                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }


            for (int i = 0; i < 26; i++) {
                for (int j = 0; j < 26; j++) {
                    PushChunk chunk = chunks[i][j];
                    int size = chunk.getSize();
                    long[] data = chunk.getData();
                    byte cat = (byte) (i + 'a');
                    byte second = (byte) (j + 'a');
                    ByteBuffer sendBuffer = ByteBuffer.allocate(BUFSIZE);
                    Arrays.sort(data, 0, size);
                    sendBuffer.put(cat);
                    sendBuffer.put(second);
                    for (int count = 0; count < size; count++) {
                        sendBuffer.putLong(data[count]);
                    }
                    sendBuffer.flip();
                    socket.sendByteBuffer(sendBuffer, 0);
                }
            }
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("Waiting for executor to terminate");
            }
            socket.send("");
            socket.close();
        }
    }
}
