package commands.impl.geospatial;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;

public class GeoAddCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
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

        return CommandResponse.integer(1);
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
