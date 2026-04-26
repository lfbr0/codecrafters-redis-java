package commands.impl.geospatial;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import commands.impl.sortedset.RedisSortedSet;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

public class GeoPosCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() < 2) {
                throw new IllegalArgumentException("GEOPOS expects at least 2 arguments (key, member)!");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            for (int i = 1; i < context.getArguments().size(); i++) {
                RedisMessage memberRaw = context.getArguments().get(i);
                if (keyRaw.getType() != memberRaw.getType() || keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                    throw new IllegalArgumentException("GEOPOS arguments must be BULK STRING!");
                }
            }

            String key = keyRaw.getContent().toString();
            List<RedisMessage> resultArray = new ArrayList<>(context.getArguments().size() - 1);

            for (int i = 1; i < context.getArguments().size(); i++) {
                RedisMessage memberRaw = context.getArguments().get(i);
                Optional<RedisSortedSet.RedisSortedSetEntry> optionalEntry = MemoryManager
                        .getMemberFromSortedSet(key, memberRaw.getContent().toString());

                if (optionalEntry.isPresent()) {
                    GeoCoordinates geoCoordinates = GeoCoordinates.decode(((Double) optionalEntry.get().score()).longValue());

                    RedisMessage longitudeMsg = new RedisMessage();
                    longitudeMsg.setType(RedisMessage.RedisMessageType.BULK_STRING);
                    longitudeMsg.setContent(Double.valueOf(geoCoordinates.longitude()).toString());

                    RedisMessage latitudeMsg = new RedisMessage();
                    latitudeMsg.setType(RedisMessage.RedisMessageType.BULK_STRING);
                    latitudeMsg.setContent(Double.valueOf(geoCoordinates.latitude()).toString());

                    RedisMessage innerArray = new RedisMessage();
                    innerArray.setType(RedisMessage.RedisMessageType.ARRAY);
                    innerArray.setContent(List.of(longitudeMsg, latitudeMsg));
                    resultArray.add(innerArray);
                } else {
                    // For missing members, add a null array
                    RedisMessage nullArray = new RedisMessage();
                    nullArray.setType(RedisMessage.RedisMessageType.ARRAY);
                    nullArray.setContent(null);
                    resultArray.add(nullArray);
                }
            }

            RedisMessage wrapperArray = new RedisMessage();
            wrapperArray.setType(RedisMessage.RedisMessageType.ARRAY);
            wrapperArray.setContent(resultArray);
            return new CommandResponse(RedisSerializer.serialize(wrapperArray));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "GEOPOS".equalsIgnoreCase(commandName);
    }
}
