package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

public class TypeCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().isEmpty()) {
            throw new IllegalArgumentException("Missing key argument for TYPE command");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("Expected a bulk string for key argument, but got: " + keyRaw.getType());
        }

        String key = (String) keyRaw.getContent();
        RedisMessage value = MemoryManager.get(key);

        if (value == null) {
            context.getOutputStream().write(RedisSerializer.simpleString("none"));
        } else {
            String type;
            switch (value.getType()) {
                case BULK_STRING:
                case SIMPLE_STRING:
                    type = "string";
                    break;
                case ARRAY:
                    type = "list";
                    break;
                default:
                    type = "unknown";
            }
            context.getOutputStream().write(RedisSerializer.simpleString(type));
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "type".equalsIgnoreCase(commandName);
    }
}
