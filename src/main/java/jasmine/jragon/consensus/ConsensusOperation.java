package jasmine.jragon.consensus;

import com.google.common.hash.Hashing;
import jasmine.jragon.MultiClient;
import jasmine.jragon.command.ProtocolCommand;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static jasmine.jragon.command.ProtocolCommand.BEGIN_TRANSACTION;
import static jasmine.jragon.response.ServerResponse.ACKNOWLEDGED;
import static jasmine.jragon.response.ServerResponse.KEY_DOES_NOT_EXIST_IN_QUORUM;
import static jasmine.jragon.response.ServerResponse.SERVER_IO_ERROR;
import static jasmine.jragon.response.ServerResponse.SUCCESS;
import static jasmine.jragon.response.ServerResponse.TIMEOUT;
import static jasmine.jragon.response.ServerResponse.UNSUPPORTED_COMMAND;

@SuppressWarnings("unchecked")
public final class ConsensusOperation {
    private static final Logger LOG = LoggerFactory.getLogger(ConsensusOperation.class);

    private static final int TIMEOUT_NUMBER = 30;
    private static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;

    private final List<MultiClient.ServerConnection> servers;
    private final Map<String, List<MultiClient.ServerConnection>> quorumDistribution;

    @Getter
    private boolean isDone;

    public ConsensusOperation(@NonNull ProtocolCommand protocol,
                              @NonNull String[] keys,
                              @NonNull List<MultiClient.ServerConnection> servers) {
        int consensusSize = createConsensusSize(servers.size());

        this.servers = servers;
        quorumDistribution = Arrays.stream(keys)
                .map(key -> createQuorum(consensusSize, key, servers))
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));

        this.isDone = protocol != BEGIN_TRANSACTION;
    }

    private static int createConsensusSize(int serverCount) {
        return (serverCount >> 1) + 1;
    }

    private static Map.Entry<String, List<MultiClient.ServerConnection>> createQuorum(int consensusSize,
                                                                                      String key,
                                                                                      List<MultiClient.ServerConnection> servers) {
        var quorum = servers.stream()
                .map(server -> createHashServerPair(key, server))
                .sorted(compareReverseHashOrder())
                .limit(consensusSize)
                .map(Map.Entry::getValue)
                .toList();

        return Map.entry(key, quorum);
    }

    private static Map.Entry<Integer, MultiClient.ServerConnection> createHashServerPair(String key,
                                                                                         MultiClient.ServerConnection server) {
        int hash = conductKeyHash(key, server.getIpAddress(), server.getPort());

        return Map.entry(hash, server);
    }

    private static int conductKeyHash(String key, String ipAddress, int port) {
        return Hashing.murmur3_32_fixed()
                .hashString(String.format("%s%s:%d", key, ipAddress, port), StandardCharsets.UTF_8)
                .asInt();
    }

    private static Comparator<Map.Entry<Integer, ?>> compareReverseHashOrder() {
        Comparator<Map.Entry<Integer, ?>> s = Comparator.comparingInt(Map.Entry::getKey);
        return s.reversed();
    }

    public String sendCommandToServers(@NonNull ProtocolCommand protocol,
                                       @NonNull String originalCommand,
                                       @NonNull String[] keys) {

        return switch (protocol) {
            case READ, WRITE, CHECK -> sendKeyedCommandToServer(originalCommand, keys[0]);
            case BEGIN_TRANSACTION -> startTransaction(originalCommand, keys);
            case ABORT_TRANSACTION, COMMIT_TRANSACTION, SHUTDOWN_SERVER -> sendKeylessCommand(originalCommand);
            default -> UNSUPPORTED_COMMAND.toString();
        };
    }

    private String sendKeyedCommandToServer(String originalCommand,
                                            String key) {
        if (!quorumDistribution.containsKey(key)) {
            return KEY_DOES_NOT_EXIST_IN_QUORUM.toString();
        }

        CompletableFuture<String>[] responseFutures = quorumDistribution.get(key)
                .stream()
                .map(server -> operateOnSingleServer(originalCommand, server.getBuffer(), server.getServerConnection()))
                .toArray(CompletableFuture[]::new);

        return waitForResponse(responseFutures, originalCommand);
    }

    private String startTransaction(String originalCommand, String[] keys) {
        if (quorumDistribution.size() == 1) {
            return sendKeyedCommandToServer(originalCommand, keys[0]);
        }

        var prefix = BEGIN_TRANSACTION.getCommandName() + " ";

        var serverTransactionCommands = quorumDistribution.entrySet()
                .stream()
                .mapMulti(reversePairing())
                .collect(Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(
                                Map.Entry::getValue,
                                Collectors.joining(" ", prefix, "")
                        )
                ));

        LOG.trace("Transaction Map: {}", serverTransactionCommands);

        CompletableFuture<String>[] responseFutures = serverTransactionCommands.entrySet()
                .stream()
                //Filter out empty transaction commands
                .filter(e -> !prefix.equals(e.getValue()))
                .map(e -> operateOnSingleServer(
                        e.getValue(),
                        e.getKey().getBuffer(),
                        e.getKey().getServerConnection()))
                .toArray(CompletableFuture[]::new);

        return waitForResponse(responseFutures, originalCommand);
    }

    private static BiConsumer<Map.Entry<String, List<MultiClient.ServerConnection>>,
            Consumer<Map.Entry<MultiClient.ServerConnection, String>>> reversePairing() {
        return (entry, consumer) -> {
            for (var server : entry.getValue()) {
                consumer.accept(Map.entry(server, entry.getKey()));
            }
        };
    }

    private String sendKeylessCommand(String originalCommand) {
        CompletableFuture<String>[] responseFutures = servers.stream()
                .map(server -> operateOnSingleServer(originalCommand, server.getBuffer(), server.getServerConnection()))
                .toArray(CompletableFuture[]::new);

        var response = waitForResponse(responseFutures, originalCommand);

        //The only 2 scenarios where a transaction closes
        isDone = response.startsWith(SUCCESS.toString()) ||
                ACKNOWLEDGED.toString().equals(response);
        return response;
    }

    private static String waitForResponse(CompletableFuture<String>[] responseFutures, String originalCommand) {
        try {
            CompletableFuture.allOf(responseFutures).get(TIMEOUT_NUMBER, TIMEOUT_UNIT);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            LOG.error("Sending command [{}] to server failed", originalCommand, e);
            return TIMEOUT.toString();
        }

        var responseFrequencies = Arrays.stream(responseFutures)
                .map(CompletableFuture::join)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        LOG.trace("Command [{}] produced vote {}", originalCommand, responseFrequencies);

        return responseFrequencies.entrySet()
                .stream()
                .max(Comparator.comparingLong(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .orElse("If there are servers, this should be unreachable");
    }

    private static CompletableFuture<String> operateOnSingleServer(String command,
                                                                   ByteBuffer buffer,
                                                                   SocketChannel serverSocket) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                buffer.clear()
                        .put(command.getBytes(StandardCharsets.UTF_8))
                        .flip();
                while (buffer.hasRemaining()) {
                    serverSocket.write(buffer);
                }

                buffer.clear();

                int n = serverSocket.read(buffer);
                buffer.flip();

                return new String(buffer.array(), buffer.position(), n);
            } catch (IOException e) {
                LOG.warn("Command [{}] couldn't be processed", command);
                return SERVER_IO_ERROR.toString();
            }
        });
    }
}
