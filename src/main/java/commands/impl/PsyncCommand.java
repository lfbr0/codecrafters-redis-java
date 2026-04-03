package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import replication.ReplicationManager;
import serdes.RedisSerializer;

public class PsyncCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        // ignore all arguments for now
        String replicationIdStr = "FULLRESYNC " + ReplicationManager.getReplicationId() + " 0";
        return new CommandResponse(RedisSerializer.simpleString(replicationIdStr));
    }

    @Override
    public boolean matches(String commandName) {
        return "psync".equalsIgnoreCase(commandName);
    }
}
