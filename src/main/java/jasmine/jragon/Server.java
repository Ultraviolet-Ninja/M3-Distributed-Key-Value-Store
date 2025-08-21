package jasmine.jragon;

import jasmine.jragon.client.TreeClient;
import jasmine.jragon.response.ServerResponse;
import jasmine.jragon.tree.BTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class Server {
    public static final int PORT = 8080;
    public static final int BUFFER_SIZE = 1024;

    private static final AtomicLong CLIENT_COUNTER = new AtomicLong(-1);
    private static final AtomicLong SERVER_COUNT = new AtomicLong(-1);

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private static final String RECONSTRUCTION_FILE = "tree-log.txt";

    public static void main(String[] args) {
        BTree serverTree = null;
        Map<SelectableChannel, TreeClient> connectionMap = new HashMap<>();
        long serverId = SERVER_COUNT.incrementAndGet();
        try {
            serverTree = startServer(args, serverId, connectionMap);
        } catch (IOException e) {
            LOG.error("Server {} Internal Error Occurred: ", serverId, e);
        } finally {
            if (serverTree != null) {
                serverTree.shutdownGracefully();
            }
        }
    }

    private static BTree startServer(String[] args, long serverId, Map<SelectableChannel, TreeClient> connectionMap) throws IOException {
        BTree serverTree;
        LOG.debug("Starting Server {}", serverId);
        var isServerShutdown = new AtomicBoolean(false);

        try (var selector = Selector.open();
             var server = ServerSocketChannel.open()) {

            int port = PORT;
            File reconstructionFile;
            if (args.length == 1) {
                try {
                    port = Integer.parseInt(args[0]);
                } catch (NumberFormatException e) {
                    LOG.error("Invalid port number: {}. Using {} as the default", args[0], PORT);
                }
                server.bind(new InetSocketAddress(port));
                reconstructionFile =  new File(port + "-" + RECONSTRUCTION_FILE);
            } else {
                reconstructionFile = new File(PORT + "-" + RECONSTRUCTION_FILE);
                server.bind(new InetSocketAddress(port));
            }
            if (!reconstructionFile.exists()) {
                //noinspection ResultOfMethodCallIgnored
                reconstructionFile.createNewFile();
            }

            serverTree = new BTree(5, reconstructionFile);


            LOG.debug("Listening on port {}", port);

            server.configureBlocking(false);
            server.register(selector, SelectionKey.OP_ACCEPT);

            Set<String> globalKeyLock = new HashSet<>();


            while (isRunning(isServerShutdown, connectionMap)) {
                if (selector.select() != 0) {
                    for (var selectionKey : selector.selectedKeys()) {
                        if (selectionKey.isAcceptable()) {
                            acceptIncoming(selectionKey.channel(), selector, serverTree, globalKeyLock, connectionMap);
                        } else if (selectionKey.isReadable()) {
                            var client = selectionKey.channel();
                            try {
                                readIncomingCommand(client, isServerShutdown, connectionMap);
                            } catch (IOException e) {
                                LOG.error("Unexpected Drop of connection: {}", e.getMessage());
                                removeClient(client, connectionMap);
                            }
                        }
                    }

                    selector.selectedKeys().clear();
                }
            }
        }

        return serverTree;
    }

    private static void acceptIncoming(SelectableChannel acceptedChannel, Selector selector,
                                       BTree serverTree, Set<String> globalKeyLock,
                                       Map<SelectableChannel, TreeClient> connectionMap)
            throws IOException {
        if (acceptedChannel instanceof ServerSocketChannel channel) {
            var client = channel.accept();
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ);

            var treeClient = TreeClient.from(
                    CLIENT_COUNTER.incrementAndGet(),
                    globalKeyLock,
                    serverTree,
                    client
            );
            connectionMap.put(client, treeClient);
            System.out.println("Accepted connection from " + treeClient);
        }
    }

    private static void readIncomingCommand(SelectableChannel incomingChannel,
                                            AtomicBoolean isServerShutdown,
                                            Map<SelectableChannel, TreeClient> connectionMap)
            throws IOException {
        if (incomingChannel instanceof SocketChannel client) {
            var buffer = ByteBuffer.allocate(BUFFER_SIZE);
            int n = client.read(buffer);

            var response = "";
            if (n == -1) {
                removeClient(client, connectionMap);
                LOG.trace("Client closed");
                return;
            } else if (!connectionMap.containsKey(client)) {
                LOG.warn("Client not found");
                response = ServerResponse.UNKNOWN_CLIENT.toString();
            } else {
                buffer.flip();
                var request = new String(buffer.array(), buffer.position(), n)
                        .trim();

                response = connectionMap.get(client)
                        .acceptCommand(request, isServerShutdown);
            }

            buffer.clear();
            buffer.put(response.getBytes());
            buffer.flip();

            while (buffer.hasRemaining()) {
                client.write(buffer);
            }
        }
    }

    private static void removeClient(SelectableChannel client,
                                     Map<SelectableChannel, TreeClient> connectionMap)
            throws IOException {
        var internalClient = connectionMap.remove(client);
        client.close();

        if (internalClient != null && !internalClient.isDone()) {
            internalClient.eraseTransaction();
        }
    }

    private static boolean isRunning(AtomicBoolean isServerShutdown,
                                     Map<SelectableChannel, TreeClient> connectionMap) {
        return !(isServerShutdown.get() &&
                connectionMap.values()
                        .stream()
                        .allMatch(TreeClient::isDone));
    }
}