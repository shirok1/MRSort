# 基于 Java 与 ZeroMQ 的分布式排序系统

数学三班 第二组

## 问题分析

经过测试，发现处理的瓶颈在于输入输出（主要是硬盘的读写），而不是在排序的算法上。因此，选择将更多的精力用在优化输入输出的过程。

## 技术选型

Java vs. Python：Python 有全局解释器锁（GIL），难以实现多线程处理，多进程控制困难，内置的异步系统不够透明；Java 的异步写入可以通过线程池实现自行控制。

Java Socket vs. Netty vs. ZeroMQ：Java 自带的 Socket 无溢出保护；Netty 有异步但晦涩难懂，且瓶颈显然在传输上而不是在接收上；ZeroMQ 的 API 足够简洁且性能开销很小。

## 数据压缩

为了降低硬盘的读写量，希望将数据压缩为更小的文件。
注意到这些字符串均由 15 个小写字母（和一个 `\n`）组成，而 26**13 = 2.48E18 < 9.22E18 = Long.MAX_VALUE。
因此可以将字符串的后 13 个字母通过进制转换压缩为一个 8 字节的长整数，再另外存储前 2 个字母。
假如以这 2 个字母为索引，不实际存入硬盘，则可忽略这 2 个字母占用的空间，使得原本占用 16 字节的字符串可以压缩为 8 字节。
同时，为了确保压缩后仍然可以以长整数的形式进行比较，还要确保进制转换时靠前的字母在高位。

## 代码流程解析

### 发送方

```java
PushChunk[][] chunks = new PushChunk[26][26];
for (int i = 0; i < 26; i++) {
    for (int j = 0; j < 26; j++) {
        chunks[i][j] = new PushChunk(CAP);
    }
}
```

按字符串的前 2 个字母分组分批。

```java
ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>());
try (ZContext context = new ZContext(8)) {
    org.zeromq.ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
    socket.connect("tcp://localhost:5555");
    ByteBuffer buffer = ByteBuffer.allocate(16 * 1024);
```

初始化线程池、Socket 和（读文件用的）缓冲区。

```java
for (Path file : files) {
    try (SeekableByteChannel fc = Files.newByteChannel(file, READ)) {
        int count = 0;
        // ...
        while (true) {
            int read = fc.read(buffer);
            if (read == -1) break;
            buffer.flip();
            // ...
```

从硬盘读入缓冲区。

```java
for (int position = 0; position < read; position += 16) {
    byte cat = buffer.get(position);
    byte second = buffer.get(position + 1);
    long rest = 0L;
    for (int i = 2; i < 15; i++) {
        rest = rest * 26 + buffer.get(position + i) - 'a';
    }
    PushChunk chunk = chunks[cat - 'a'][second - 'a'];
    chunk.add(rest);
    if (chunk.getSize() == CAP) {
        int size = chunk.getSize();
        long[] data = chunk.getAndReset(CAP);
        Thread.sleep(2);
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
buffer.clear();
// ...}}
```

将缓冲区中的字符串转换为长整数，检测对应的前 2 个字母的分组是否已达到设定的上限，达到则将其整批发送给服务器。

```java
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
```

清理未发送的字符串，并发送结束标志。

### 接收方

```java
try (ZContext context = new ZContext()) {
    CatCombo[] sinks = new CatCombo[parameter.end - parameter.start + 1];
    for (char i = parameter.start; i <= parameter.end; i++) {
        LOG.debug("Creating sink for cat {}", i);
        sinks[i - parameter.start] = new CatCombo((byte) i);
    }

    Socket socket = context.createSocket(SocketType.PULL);
    socket.bind("tcp://*:" + parameter.port);

    LOG.info("Ready for PUSH! Port: {}", parameter.port);

    ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>());

    ByteBuffer buffer = ByteBuffer.allocate(Pusher.BUFSIZE);
```

初始化内存、Socket，分配从网络读入用的缓冲区（注意这里读入缓冲区的大小和发送方的发送缓冲区保持一致）

```java
while (!Thread.currentThread().isInterrupted()) {
    int size = socket.recvByteBuffer(buffer, 0);
    if (size < 1) {
        LOG.info("Received empty string!");
        break;
    }
    buffer.flip();
    byte cat = buffer.get(0);
    byte sec = buffer.get(1);

    CatCombo catCombo = sinks[cat - parameter.start];
    CatCombo.SecondCombo secondCombo = catCombo.secondCombos[sec - 'a'];
    PersistedFile persisted = PersistedFile.fromInput(buffer, secondCombo.counter, parameter.cache);
    executor.execute(() ->
        fileCreated(cat, sec, secondCombo.queue, secondCombo.counter, executor, parameter.cache)
            .accept(persisted));
    LOG.info("Created file {}, executor {} waiting, {} running.",
        persisted.getPath().getFileName(), executor.getQueue().size(), executor.getActiveCount());
    buffer.clear();
}
```

循环接受信息，去掉表示前 2 个字母的字节后**直接将长整数写入硬盘**，并触发文件创建的事件。

```java
private static Consumer<PersistedFile> fileCreated(byte cat, byte sec, Set<PersistedFile> q, AtomicLong counter, Executor ex, Path cacheDir) {
    return file -> {
        synchronized (q) {
            Optional<PersistedFile> best = q.stream().filter(sameLevel(file)).findFirst();
            if (!best.isPresent())
                best = q.stream().filter(levelInRange(file)).findFirst();
            if (!best.isPresent()) {
                LOG.info("No best match, adding {} to [{}]", file.getFileName(),
                    q.stream().map(PersistedFile::getFileName).collect(Collectors.joining(", ")));
                q.add(file);
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
```

文件创建后，查询是否有头 2 个字母相同、Level 相近的文件，有则将它们合并，合并后再次触发文件创建的事件。

```java
socket.close();
LOG.info("Waiting for thread pool to empty...");
while (executor.getQueue().size() != 0 && executor.getActiveCount() != 0) {
    LOG.info("Still waiting...({} waiting, {} running)",
            executor.getQueue().size(), executor.getActiveCount());
    Thread.sleep(500);
}
executor.shutdown();
while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
    LOG.info("Waiting for final termination...");
}
```

等待线程池完全关停。

```java
for (CatCombo catCombo : sinks) {
    byte cat = catCombo.category;
    List<FileTuple> result = Arrays.stream(catCombo.secondCombos).map(c -> c.queue.parallelStream()
                    .reduce((a, b) -> PersistedFile.sortMerge
                            (cat, c.second, a, b, c.counter, parameter.cache)).map(f -> new FileTuple(f, c.second)))
            .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    LOG.info("String started with {} merged, now decompressing.", (char) cat);
    Path target = parameter.result.resolve("result" + (char) cat + ".txt");
    try (SeekableByteChannel outc = Files.newByteChannel(target, WRITE, CREATE, TRUNCATE_EXISTING)) {
        for (FileTuple tpl : result) tpl.file.decompress(outc, cat, tpl.second);
        LOG.info("Decompressed to {}.", target.getFileName());
    } catch (IOException e) {
        LOG.error("Failed to decompress {}: {}", target.getFileName(), e);
    }
}
```

强制合并未合并的文件，最后解压缩为目标的结果格式。
