package jasmine.jragon;

import jasmine.jragon.command.ProtocolCommand;
import jasmine.jragon.consensus.ConsensusOperation;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.IntStream;

import static jasmine.jragon.response.ServerResponse.INSUFFICIENT_ARGUMENTS;

public final class MultiClient {
    private static final Logger LOG = LoggerFactory.getLogger(MultiClient.class);

    private static ConsensusOperation currentOperation = null;

    public static void main(String[] args) {
        var userIn = new Scanner(System.in);

        var servers = IntStream.iterate(0, i -> i + 2)
                .limit(args.length >> 1)
                .mapToObj(i -> ServerConnection.from(args[i], Integer.parseInt(args[i + 1])))
                .flatMap(Optional::stream)
                .toList();

        while (true) {
            System.out.println("Enter command:");
            var command = userIn.nextLine().trim();

            if (command.equalsIgnoreCase("exit")) break;

            var components = command.split("\\s+");
            var protocol = ProtocolCommand.lookup(components[0]);

            if (protocol.containsSufficientArguments(components.length-1)) {
                var response = conductOperation(components, protocol, command, servers);

                if (currentOperation != null && currentOperation.isDone()) {
                    currentOperation = null;
                }

                System.out.println("Server response: " + response);
            } else if (protocol == ProtocolCommand.UNSUPPORTED) {
                System.out.println("Unsupported protocol: " + command);
            } else {
                System.out.println("Server response: " + INSUFFICIENT_ARGUMENTS);
            }
        }

        closeConnections(servers);
    }

    private static String conductOperation(String[] components,
                                           ProtocolCommand protocol,
                                           String command,
                                           List<ServerConnection> servers) {
        String[] keys;

        switch (protocol) {
            case READ, WRITE, CHECK -> keys = new String[]{components[1]};
            case BEGIN_TRANSACTION -> {
                keys = new String[components.length-1];
                System.arraycopy(components, 1, keys, 0, keys.length);
            }
            default -> keys = new String[0];
        }

        if (currentOperation == null || currentOperation.isDone()) {
            currentOperation = new ConsensusOperation(protocol, keys, servers);
        }

        return currentOperation.sendCommandToServers(protocol, command, keys);
    }

    private static void closeConnections(Collection<ServerConnection> channels) {
        for (var channel : channels) {
            try {
                channel.close();
            } catch (IOException e) {
                LOG.warn("Socket Closure Failure: ", e);
            }
        }
    }

    @Getter
    @EqualsAndHashCode(of = {"ipAddress", "port"}, doNotUseGetters = true)
    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class ServerConnection implements AutoCloseable {
        private final String ipAddress;
        private final int port;
        private final SocketChannel serverConnection;
        private final ByteBuffer buffer;

        private boolean closed;

        static Optional<ServerConnection> from(String ipAddress, int port) {
            var connection = createSocketChannel(ipAddress, port);

            return connection.map(socket -> new ServerConnection(
                    ipAddress,
                    port,
                    socket,
                    ByteBuffer.allocate(Server.BUFFER_SIZE),
                    false
            ));
        }

        @Override
        public void close() throws IOException {
            serverConnection.close();
            closed = true;
        }

        @Override
        public String toString() {
            return String.format("Connection[%s:%d]", ipAddress, port);
        }
    }

    private static Optional<SocketChannel> createSocketChannel(String ipAddress, int port) {
        try {
            var s = SocketChannel.open();

            s.connect(new InetSocketAddress(ipAddress, port));
            s.configureBlocking(true);

            return Optional.of(s);
        } catch (IOException e) {
            LOG.warn("Socket Creation Failed: ", e);
            return Optional.empty();
        }
    }
}
