package c3g2.mrsort;

import org.zeromq.SocketType;
import org.zeromq.ZContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

public class Pusher {
    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {

            org.zeromq.ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
            socket.connect("tcp://localhost:5555");

            try (FileInputStream f = new FileInputStream("C:\\Users\\Shiroki\\Code\\MRSort\\data01.txt")) {
                Scanner scanner = new Scanner(f);
                while (scanner.hasNext()) {
                    String next = scanner.next();
                    socket.send(next);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            socket.send("");
        }
    }
}
