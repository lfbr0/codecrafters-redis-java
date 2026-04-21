package commands.impl.replication;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import replication.ReplicationManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class InfoCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        String optionalSection = null;
        if (context.getArguments() != null && !context.getArguments().isEmpty()) {
            RedisMessage optionalSectionRaw = context.getArguments().getFirst();

            if (optionalSectionRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING &&
                optionalSectionRaw.getType() != RedisMessage.RedisMessageType.SIMPLE_STRING) {
                throw new IllegalArgumentException("Expected section to be string!");
            }

            optionalSection = (String) optionalSectionRaw.getContent();
        }

        // check if optional section exists, otherwise get all
        String infoStr = ReplicationManager.getInfo();
        return new CommandResponse(RedisSerializer.bulkString(infoStr));
    }

    @Override
    public boolean matches(String commandName) {
        return "info".equalsIgnoreCase(commandName);
    }
}
