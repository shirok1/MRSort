package c3g2.mrsort;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        switch (args[0]) {
            case "pusher":
                Pusher.main(Arrays.stream(args).skip(1).toArray(String[]::new));
                break;
            case "sink":
                Sink.main(Arrays.stream(args).skip(1).toArray(String[]::new));
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + args[0]);
        }
    }
}
