package commands.impl.geospatial;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import commands.impl.sortedset.RedisSortedSet;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Callable;

public class GeoSearchCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 7) {
                throw new IllegalArgumentException("GEOSEARCH right now only supports FROMLONLAT and BYRADIUS arguments, " +
                        "example: GEOSEARCH places FROMLONLAT 2 48 BYRADIUS 100 m");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("GEOSEARCH key must be bulk string!");
            }

            RedisMessage radiusUnitRaw = context.getArguments().get(6);
            if (!radiusUnitRaw.getContent().toString().equalsIgnoreCase("m")) {
                throw new IllegalArgumentException("GEOSEARCH only supports meters at the moment!");
            }

            RedisMessage lonRaw = context.getArguments().get(2);
            RedisMessage latRaw = context.getArguments().get(3);
            double lon = Double.parseDouble(lonRaw.getContent().toString());
            double lat = Double.parseDouble(latRaw.getContent().toString());

            RedisMessage radiusRaw = context.getArguments().get(5);
            double radius = Double.parseDouble(radiusRaw.getContent().toString());

            String key = keyRaw.getContent().toString();
            GeoCoordinates thisLocation = new GeoCoordinates(lat, lon);

            List<String> locations = MemoryManager
                    .getSortedSet(key)
                    .stream()
                    .filter(entry -> {
                        GeoCoordinates location = GeoCoordinates.decode(entry.score().longValue());
                        return thisLocation.distanceTo(location) <= radius;
                    })
                    .map(RedisSortedSet.RedisSortedSetEntry::member)
                    .toList();

            return new CommandResponse(RedisSerializer.listStrings(locations));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "GEOSEARCH".equalsIgnoreCase(commandName);
    }
}
