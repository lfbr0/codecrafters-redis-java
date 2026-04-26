package commands.impl.sortedset;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class ZRemCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 2) {
                throw new IllegalArgumentException("ZREM expects exactly 2 commands (key, member)!");
            }

            RedisMessage keyRaw = context.getArguments().getFirst();
            RedisMessage memberRaw = context.getArguments().getLast();
            if (keyRaw.getType() != memberRaw.getType() || memberRaw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("ZREM expects both arguments to be BULK STRING!");
            }

            String key = keyRaw.getContent().toString();
            String member = memberRaw.getContent().toString();
            boolean removed = MemoryManager.removeMemberFromSortedSet(key, member);
            return removed ? CommandResponse.integer(1) : CommandResponse.integer(0);
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "ZREM".equalsIgnoreCase(commandName);
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}
