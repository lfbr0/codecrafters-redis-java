package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class RpushCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() < 2) {
            throw new IllegalArgumentException("RPUSH command requires at least 2 arguments: key and value(s)");
        }

        // TODO: prepare for transcation support
        RedisMessage[] args = new RedisMessage[context.getArguments().size() - 1];
        for (int i = 1; i < context.getArguments().size(); i++) {
            args[i - 1] = context.getArguments().get(i);
        }
        String key = (String) context.getArguments().getFirst().getContent();
        int newSize = MemoryManager.pushToList(key, args);
        context.getOutputStream().write(RedisSerializer.integer(newSize));
    }

    @Override
    public boolean matches(String commandName) {
        return "rpush".equalsIgnoreCase(commandName);
    }
}
