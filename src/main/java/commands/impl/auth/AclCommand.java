package commands.impl.auth;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

public class AclCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 1) {
                throw new IllegalArgumentException("ACL command expects exactly 1 argument!");
            }

            RedisMessage aclArgRaw = context.getArguments().getFirst();
            if (aclArgRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("ACL command argument should be BULK STRING!");
            }

            String aclArg = aclArgRaw.getContent().toString();
            if (aclArg.equalsIgnoreCase("WHOAMI")) {
                return CommandResponse.bulkString("default");
            }

            throw new IllegalArgumentException("ACL command argument is invalid!");
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "ACL".equalsIgnoreCase(commandName);
    }
}
