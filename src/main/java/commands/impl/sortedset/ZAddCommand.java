package commands.impl.sortedset;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

public class ZAddCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 3) {
                throw new IllegalArgumentException("ZADD command requires exactly 3 argument");
            }

            RedisMessage zSetKeyRaw = context.getArguments().getFirst();
            RedisMessage zSetScoreRaw = context.getArguments().get(1);
            RedisMessage zSetMemberRaw = context.getArguments().getLast();
            if (zSetKeyRaw.getType() != zSetScoreRaw.getType() ||
                    zSetMemberRaw.getType() != zSetScoreRaw.getType() ||
                    zSetScoreRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("Command only accepts bulk strings!");
            }

            String zSetKey = zSetKeyRaw.getContent().toString();
            double zSetScore = Double.parseDouble(zSetScoreRaw.getContent().toString());
            String zSetMember = zSetMemberRaw.getContent().toString();

            // if added = 1; if not = 0
            int result = MemoryManager.addToSortedSet(zSetKey, zSetMember, zSetScore) ? 1 : 0;
            return new CommandResponse(RedisSerializer.integer(result));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "zadd".equalsIgnoreCase(commandName);
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}
