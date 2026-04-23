package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.PersistenceManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;

public class ConfigCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() < 2) {
            throw new IllegalArgumentException("CONFIG expects at least two arguments!");
        }

        RedisMessage argumentRaw = context.getArguments().getFirst();
        if (argumentRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("CONFIG argument expects a string!");
        }

        String argument = argumentRaw.getContent().toString();

        if ("GET".equalsIgnoreCase(argument)) {
            String fieldToGet = context.getArguments().get(1).getContent().toString();
            if ("dir".equalsIgnoreCase(fieldToGet)) {
                String dbFileDir = PersistenceManager
                        .getInstance()
                        .orElseThrow(() -> new IllegalArgumentException("GET dir cannot be performed, no persistence manager!"))
                        .getDbFileDir();

                byte[] msgBytes = RedisSerializer.listStrings(List.of("dir", dbFileDir));
                return new CommandResponse(msgBytes);
            }
            if ("dbfilename".equalsIgnoreCase(fieldToGet)) {
                String dbFilename = PersistenceManager
                        .getInstance()
                        .orElseThrow(() -> new IllegalArgumentException("GET dbfilename cannot be performed, no persistence manager!"))
                        .getDbFilename();

                byte[] msgBytes = RedisSerializer.listStrings(List.of("dbfilename", dbFilename));
                return new CommandResponse(msgBytes);
            }
        }

        throw new IllegalArgumentException("CONFIG did not receive valid arguments!");
    }

    @Override
    public boolean matches(String commandName) {
        return "CONFIG".equalsIgnoreCase(commandName);
    }
}
