package commands.impl.replication;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import logger.Logger;
import replication.ReplicationManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.*;

public class WaitCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 2) {
                throw new IllegalArgumentException("Wrong number of arguments!");
            }

            RedisMessage numReplicasRaw = context.getArguments().getFirst();
            RedisMessage timeoutRaw = context.getArguments().getLast();
            if (numReplicasRaw.getType() != timeoutRaw.getType() && timeoutRaw.getType() != INTEGER) {
                throw new IllegalArgumentException("Arguments received were not of expected type!");
            }

            int numReplicas = Integer.parseInt(numReplicasRaw.getContent().toString());
            long timeout = Long.parseLong(timeoutRaw.getContent().toString());
            Logger.info("Performing wait command with numReplicas=" + numReplicas + ", timeout=" + timeout);

            long ackedReplicas = ReplicationManager.getReplicaCount(timeout);
            return new CommandResponse(RedisSerializer.integer(ackedReplicas));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "wait".equalsIgnoreCase(commandName);
    }
}
