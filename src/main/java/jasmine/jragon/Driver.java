package jasmine.jragon;

import jasmine.jragon.generate.PairCreation;
import jasmine.jragon.network.ServerDevice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.runAsync;

public final class Driver {
    private static final Logger LOG = LoggerFactory.getLogger(Driver.class);
    private static final String CONFIG_FILE = "config.yml";
    private static final String DEFAULT_CONFIG = "self";
    private static final ServerDevice CURRENT_DEVICE = ServerDevice.LAPTOP;

    public static void main(String[] args) {
        if (args.length > 0) {
            switch (args[0].toLowerCase()) {
                case "-ms", "--multi-server" -> {
                    var configName = args.length > 1 ? args[1] : DEFAULT_CONFIG;
                    var loadedServers = parseServersFromYml(configName, false)
                            .stream()
                            .map(connectionArray -> connectionArray[1])
                            .map(port -> runAsync(() -> Server.main(new String[]{port})))
                            .toArray(CompletableFuture[]::new);

                    if (loadedServers.length != 0) {
                        CompletableFuture.allOf(loadedServers).join();
                    } else {
                        LOG.warn("No servers were found. Aborting.");
                    }
                }
                case "-mc", "--multi-client" -> {
                    var configName = args.length > 1 ? args[1] : DEFAULT_CONFIG;
                    var servers = parseServersFromYml(configName, true);
                    var clientArgs = servers.stream()
                            .flatMap(Arrays::stream)
                            .toArray(String[]::new);

                    MultiClient.main(clientArgs);
                }
                case "-c", "--client" -> Client.main(args);
                case "-pc" -> PairCreation.main(args);
                default -> Server.main(args);
            }
            return;
        }
        Server.main(args);
    }

    private static List<String[]> parseServersFromYml(String configurationName, boolean allowAll) {
        try (var inputStream = new FileInputStream(CONFIG_FILE)) {
            var yaml = new Yaml();

            Map<String, Object> configData = yaml.load(inputStream);

            if (configData.containsKey(configurationName) &&
                    configData.get(configurationName) instanceof List<?> serverList) {
                return serverList.stream()
                        .map(Object::toString)
                        .filter(l -> allowAll || ServerDevice.filterServer(l, CURRENT_DEVICE))
                        .map(ServerDevice::convertFromYml)
                        .filter(s -> s.length > 0)
                        .toList();
            }
        } catch (IOException e) {
            LOG.warn("Failed to load configuration file {}: ", CONFIG_FILE, e);
            return Collections.emptyList();
        }
        LOG.warn("Failed to find {} in {}: ", configurationName, CONFIG_FILE);

        return Collections.emptyList();
    }
}
