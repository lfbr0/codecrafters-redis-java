package commands.impl.auth;

import auth.DefaultUserAuthenticationManager;
import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class AuthCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 2) {
                throw new IllegalArgumentException("AUTH expects (user, password)!");
            }

            RedisMessage userRaw = context.getArguments().getFirst(),
                    passwordRaw = context.getArguments().getLast();
            if (userRaw.getType() != passwordRaw.getType() || userRaw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("AUTH expects both arguments to be BULK STRING!");
            }

            String user = userRaw.getContent().toString();
            if (!user.equals("default")) {
                throw new IllegalArgumentException("AUTH expects user to be default!");
            }

            String password = passwordRaw.getContent().toString();
            if (DefaultUserAuthenticationManager.getInstance().passwordMatches(password)) {
                return CommandResponse.ok();
            } else {
                return CommandResponse.error("WRONGPASS invalid username-password pair or user is disabled.");
            }
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "AUTH".equalsIgnoreCase(commandName);
    }
}
