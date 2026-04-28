package commands.impl.auth;

import auth.DefaultUserAuthenticationManager;
import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.ARRAY;
import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class AclCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().isEmpty()) {
                throw new IllegalArgumentException("ACL command expects at least 1 argument!");
            }

            RedisMessage aclArgRaw = context.getArguments().getFirst();
            if (aclArgRaw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("ACL command argument should be BULK STRING!");
            }

            String aclArg = aclArgRaw.getContent().toString();
            // return current user
            if (aclArg.equalsIgnoreCase("WHOAMI")) {
                return CommandResponse.bulkString("default");
            }
            // return user properties
            if (aclArg.equalsIgnoreCase("GETUSER")) {
                // must have 2 args
                if (context.getArguments().size() != 2) {
                    throw new IllegalArgumentException("ACL GETUSER must have only user argument!");
                }

                RedisMessage userRaw = context.getArguments().getLast();
                if (userRaw.getType() != BULK_STRING) {
                    throw new IllegalArgumentException("ACL GETUSER argument must be bulk string!");
                }

                String user = userRaw.getContent().toString();
                if (!user.equalsIgnoreCase("default")) {
                    throw new IllegalArgumentException("ACL GETUSER only supports default user at the moment!");
                }

                Optional<String> passwordHashOpt = DefaultUserAuthenticationManager.getInstance().getPasswordHash();

                RedisMessage flags = new RedisMessage().setType(BULK_STRING).setContent("flags");
                RedisMessage flagProps = new RedisMessage().setType(ARRAY).setContent(List.of());
                // no password, therefore no pass prop is active
                if (passwordHashOpt.isEmpty()) {
                    RedisMessage nopassFlagProp = new RedisMessage().setType(BULK_STRING).setContent("nopass");
                    flagProps.setContent(List.of(nopassFlagProp));
                }

                RedisMessage passwords = new RedisMessage().setType(BULK_STRING).setContent("passwords");
                RedisMessage passwordProps = new RedisMessage().setType(ARRAY).setContent(List.of());
                if (passwordHashOpt.isPresent()) {
                    passwordProps.setContent(
                            List.of(new RedisMessage().setType(BULK_STRING).setContent(passwordHashOpt.get()))
                    );
                }

                return new CommandResponse(RedisSerializer.list(flags, flagProps, passwords, passwordProps));
            }
            // set user password
            if (aclArg.equalsIgnoreCase("SETUSER")) {
                // must have 2 args
                if (context.getArguments().size() != 3) {
                    throw new IllegalArgumentException("ACL SETUSER must have 3 argument (user, password)!");
                }

                RedisMessage userRaw = context.getArguments().get(1);
                if (userRaw.getType() != BULK_STRING) {
                    throw new IllegalArgumentException("ACL SETUSER user argument must be bulk string!");
                }

                RedisMessage passwordRaw = context.getArguments().getLast();
                if (passwordRaw.getType() != BULK_STRING) {
                    throw new IllegalArgumentException("ACL SETUSER password argument must be bulk string!");
                }

                String user = userRaw.getContent().toString();
                if (!user.equals("default")) {
                    throw new IllegalArgumentException("ACL SETUSER currently only supported for default user!");
                }

                String password = passwordRaw.getContent().toString();
                if (!password.startsWith(">")) {
                    throw new IllegalArgumentException("ACL SETUSER is not using > to set password!");
                }
                // remove beggining >
                password = password.substring(1);

                DefaultUserAuthenticationManager.getInstance().setPassword(password);
                return CommandResponse.ok();
            }

            throw new IllegalArgumentException("ACL command argument is invalid!");
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "ACL".equalsIgnoreCase(commandName);
    }
}
