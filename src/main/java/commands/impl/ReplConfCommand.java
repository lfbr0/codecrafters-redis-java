package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import logger.Logger;
import replication.ReplicationManager;
import serdes.RedisMessage;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;
import static serdes.RedisMessage.RedisMessageType.INTEGER;

public class ReplConfCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 2) {
            throw new IllegalArgumentException("REPLCONF expected exactly 2 arguments!");
        }

        RedisMessage argumentFieldRaw = context.getArguments().getFirst();
        RedisMessage argumentValueRaw = context.getArguments().getLast();
        if (argumentFieldRaw.getType() != BULK_STRING) {
            throw new IllegalArgumentException("Expected argument to be Bulk String!");
        }

        String argumentField = argumentFieldRaw.getContent().toString();
        if (argumentField.equalsIgnoreCase("listening-port")) {
            int slavePort;
            if (argumentValueRaw.getType() == INTEGER) {
                slavePort = (Integer) argumentValueRaw.getContent();
            } else {
                slavePort = Integer.parseInt(argumentValueRaw.getContent().toString());
            }
            Logger.info("REPLCONF received slave port " + slavePort);

            // add to ReplicationManager that a slave is subscribed to write events
            ReplicationManager.replicateTo(slavePort);
        }

        return CommandResponse.ok();
    }

    @Override
    public boolean matches(String commandName) {
        return "replconf".equalsIgnoreCase(commandName);
    }
}
