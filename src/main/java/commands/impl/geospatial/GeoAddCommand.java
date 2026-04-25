package commands.impl.geospatial;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

public class GeoAddCommand implements Command {

    private static final int MAX_LON = 180;
    private static final int MIN_LON = -180;
    private static final Double MAX_LAT = 85.05112878;
    private static final Double MIN_LAT = -85.05112878;

    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 4) {
                throw new IllegalArgumentException("GEOADD expects exactly 4 arguments!");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            RedisMessage longitudeRaw = context.getArguments().get(1);
            RedisMessage latitudeRaw = context.getArguments().get(2);
            RedisMessage memberRaw = context.getArguments().getLast();
            if (keyRaw.getType() != longitudeRaw.getType() ||
                    longitudeRaw.getType() != latitudeRaw.getType() ||
                    latitudeRaw.getType() != memberRaw.getType() ||
                    memberRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("GEOADD arguments should be BULK STRING!");
            }

            double longitude = Double.parseDouble(longitudeRaw.getContent().toString());
            double latitude = Double.parseDouble(latitudeRaw.getContent().toString());
            if (longitude > MAX_LON || longitude < MIN_LON || latitude > MAX_LAT || latitude < MIN_LAT) {
                throw new IllegalArgumentException(getInvalidCoordsMessage(longitude, latitude));
            }

            String key = keyRaw.getContent().toString();
            String member = memberRaw.getContent().toString();
            int res = MemoryManager
                    .addToSortedSet(key, member, new GeoCoordinates(latitude, longitude).encode()) ? 1 : 0;
            return CommandResponse.integer(res);
        };
    }

    private String getInvalidCoordsMessage(Double longitude, Double latitude) {
        return String.format("ERR invalid longitude,latitude pair %s,s", longitude.toString(), latitude.toString());
    }

    @Override
    public boolean matches(String commandName) {
        return "GEOADD".equalsIgnoreCase(commandName);
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}
