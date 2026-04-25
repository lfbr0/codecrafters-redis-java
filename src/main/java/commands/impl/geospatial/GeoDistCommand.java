package commands.impl.geospatial;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;

import java.util.Optional;
import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class GeoDistCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 3) {
                throw new IllegalArgumentException("GEODIST expects exactly 3 arguments (key, member1, member2)!");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            RedisMessage m1Raw = context.getArguments().get(1);
            RedisMessage m2Raw = context.getArguments().getLast();
            if (keyRaw.getType() != m1Raw.getType() || m1Raw.getType() != m2Raw.getType() || m1Raw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("GEODIST arguments should be bulk string!");
            }

            String key = keyRaw.getContent().toString();
            String m1 = m1Raw.getContent().toString();
            String m2 = m2Raw.getContent().toString();

            Optional<GeoCoordinates> geoCoordinatesOptional1 = getCoordinates(key, m1);
            Optional<GeoCoordinates> geoCoordinatesOptional2 = getCoordinates(key, m2);

            if (geoCoordinatesOptional1.isEmpty() || geoCoordinatesOptional2.isEmpty()) {
                return CommandResponse.nullBulkString();
            }

            GeoCoordinates gc1 = geoCoordinatesOptional1.get();
            GeoCoordinates gc2 = geoCoordinatesOptional2.get();
            double distance = gc1.distanceTo(gc2);
            return CommandResponse.bulkString(Double.toString(distance));
        };
    }

    private Optional<GeoCoordinates> getCoordinates(String key, String member) {
        return MemoryManager
                .getMemberFromSortedSet(key, member)
                .map(entry -> entry.score().longValue())
                .map(GeoCoordinates::decode);
    }

    @Override
    public boolean matches(String commandName) {
        return "GEODIST".equalsIgnoreCase(commandName);
    }
}
