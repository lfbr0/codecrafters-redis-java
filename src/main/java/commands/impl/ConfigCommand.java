package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.AofPersistenceManager;
import data.RdbPersistenceManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;

public class ConfigCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
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
            // dir field
            if ("dir".equalsIgnoreCase(fieldToGet)) {
                String dbFileDir = RdbPersistenceManager.getInstance().map(RdbPersistenceManager::getDir)
                        .orElse(AofPersistenceManager.getInstance().getDir());

                byte[] msgBytes = RedisSerializer.listStrings(List.of("dir", dbFileDir));
                return new CommandResponse(msgBytes);
            }
            // dbfilename field - rdb
            if ("dbfilename".equalsIgnoreCase(fieldToGet)) {
                String dbFilename = RdbPersistenceManager
                        .getInstance()
                        .orElseThrow(() -> new IllegalArgumentException("GET dbfilename cannot be performed, no RDB persistence manager!"))
                        .getDbFilename();

                byte[] msgBytes = RedisSerializer.listStrings(List.of("dbfilename", dbFilename));
                return new CommandResponse(msgBytes);
            }
            // appendonly - aof active field
            if ("appendonly".equalsIgnoreCase(fieldToGet)) {
                boolean appendOnly = AofPersistenceManager.getInstance().isEnabled();
                byte[] msgBytes = RedisSerializer.listStrings(List.of("appendonly", appendOnly ? "yes" : "no"));
                return new CommandResponse(msgBytes);
            }
            // appenddirname - aof append dir name
            if ("appenddirname".equalsIgnoreCase(fieldToGet)) {
                String appendDir = AofPersistenceManager
                        .getInstance()
                        .getAppendDir();

                byte[] msgBytes = RedisSerializer.listStrings(List.of("appenddirname", appendDir));
                return new CommandResponse(msgBytes);
            }
            // appendfilename - The name of the append-only file that records write operations
            if ("appendfilename".equalsIgnoreCase(fieldToGet)) {
                String appendFilename = AofPersistenceManager
                        .getInstance()
                        .getAppendFilename();

                byte[] msgBytes = RedisSerializer.listStrings(List.of("appendfilename", appendFilename));
                return new CommandResponse(msgBytes);
            }
            // appendfsync - How often buffered writes are flushed to the AOF file on disk
            if ("appendfsync".equalsIgnoreCase(fieldToGet)) {
                String appendFSync = AofPersistenceManager
                        .getInstance()
                        .getAppendFSync()
                        .name()
                        .toLowerCase(Locale.ROOT);

                byte[] msgBytes = RedisSerializer.listStrings(List.of("appendfsync", appendFSync));
                return new CommandResponse(msgBytes);
            }
        }

        throw new IllegalArgumentException("CONFIG did not receive valid arguments!");
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "CONFIG".equalsIgnoreCase(commandName);
    }
}
