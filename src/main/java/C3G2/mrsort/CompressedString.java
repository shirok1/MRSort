package c3g2.mrsort;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

public class CompressedString implements Comparable<CompressedString> {
    private final byte category;
    private final byte secondChar;
    private final long rest;

    private CompressedString(byte category, byte secondChar, long rest) {
        this.category = category;
        this.secondChar = secondChar;
        this.rest = rest;
    }

    public byte getCategory() {
        return category;
    }
    public int getCategoryIndex() {
        return category - 'a';
    }

    public byte getSecondChar() {
        return secondChar;
    }

    public long getRest() {
        return rest;
    }

    public static CompressedString fromStringBuffer(ByteBuffer buffer) {
        long resultRest = 0L;
        for (int i = 2; i < 15; i++) {
            resultRest = resultRest * 26 + buffer.get(i) - 'a';
        }
        return new CompressedString(buffer.get(0), buffer.get(1), resultRest);
    }

    public void writeCompressedTo(ByteChannel dst, ByteBuffer buffer) throws IOException {
        assert buffer.position() == 0;
        assert buffer.remaining() >= 10;
        buffer.put(category);
        buffer.put(secondChar);
        buffer.putLong(rest);
        buffer.flip();
        dst.write(buffer);
        buffer.clear();
    }

    @Override
    public String toString() {
        return "CompressedString{" +
                "category=" + category +
                ", secondChar=" + secondChar +
                ", rest=" + rest +
                '}';
    }

    @Override
    public int compareTo(CompressedString o) {
        int result = Byte.compare(category, o.category);
        if (result != 0) return result;
        result = Byte.compare(secondChar, o.secondChar);
        if (result != 0) return result;
        return Long.compare(rest, o.rest);
    }

    public static int compare(ByteBuffer a, ByteBuffer b){
        int result = Byte.compare(a.get(0), b.get(0));
        if (result != 0) return result;
        result = Byte.compare(a.get(1),b.get(1));
        if (result != 0) return result;
        return Long.compare(a.getLong(2),b.getLong(2));
    }
}
