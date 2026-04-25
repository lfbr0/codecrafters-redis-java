package commands.impl.replication;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import replication.ReplicationManager;
import serdes.RedisSerializer;

import java.util.HexFormat;
import java.util.concurrent.Callable;

public class PsyncCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            // ignore all arguments for now
            String replicationIdStr = "FULLRESYNC " + ReplicationManager.getReplicationId() + " 0";

            // post response, we want to send empty RDB file
            CommandResponse.CommandPostResponseCallback postResponseCb = os -> {
                os.flush();
                byte[] contents = HexFormat.of().parseHex(
                        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
                os.write(("$" + contents.length + "\r\n").getBytes());
                os.write(contents);
            };

            return new CommandResponse(RedisSerializer.simpleString(replicationIdStr), postResponseCb);
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "psync".equalsIgnoreCase(commandName);
    }
}
