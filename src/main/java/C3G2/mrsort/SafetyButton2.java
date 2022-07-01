package c3g2.mrsort;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class SafetyButton2 {

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
            socket.connect("tcp://" + args[0] + ":5555");
            System.out.println("Manually sending stop signal...");
            socket.send("");
            socket.close();
            System.out.println("Stop signal sent.");
        }
    }
}
