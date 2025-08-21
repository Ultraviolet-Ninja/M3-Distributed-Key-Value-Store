package jasmine.jragon.network;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ServerDevice {
    //Assuming that all traffic is going to come from the daily drive laptop
    LAPTOP("self", "127.0.0.1"),
    MAC("mac", "192.168.1.186");

    private final String deviceName;
    private final String ipAddress;

    public static boolean filterServer(String ymlLine, ServerDevice serverDevice) {
        return ymlLine.startsWith(serverDevice.deviceName);
    }

    public static String[] convertFromYml(String ymlLine) {
        var split = ymlLine.split(":");
        var id = split[0];
        for (var device : values()) {
            if (device.deviceName.equals(id)) {
                return new String[]{device.ipAddress, split[1]};
            }
        }

        return new String[0];
    }
}
