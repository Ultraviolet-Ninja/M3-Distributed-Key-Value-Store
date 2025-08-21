package jasmine.jragon;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public final class Client {
    public static final String DEFAULT_HOST = "127.0.0.1";

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) {
        var userIn = new Scanner(System.in);

        try (var serverSocket = SocketChannel.open()) {
            var buffer = ByteBuffer.allocate(Server.BUFFER_SIZE);
            var ipAddress = args.length > 1 ? args[1] : DEFAULT_HOST;
            var port = args.length > 2 ? Integer.parseInt(args[2]) : Server.PORT;
            serverSocket.connect(new InetSocketAddress(ipAddress, port));
            serverSocket.configureBlocking(true);

            while (true) {
                var command = userIn.nextLine();

                if (command.equalsIgnoreCase("exit")) break;

                buffer.clear().put(command.getBytes(StandardCharsets.UTF_8)).flip();
                while (buffer.hasRemaining()) {
                    serverSocket.write(buffer);
                }

                buffer.clear();

                int n = serverSocket.read(buffer);
                buffer.flip();

                String response = new String(buffer.array(), buffer.position(), n);
                System.out.println("Server said: " + response);
            }
        } catch (IOException e) {
            LOG.error("Premature Client Shutdown: ", e);
        }
    }
}
