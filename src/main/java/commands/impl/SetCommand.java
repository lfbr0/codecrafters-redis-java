package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.time.Duration;

public class SetCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() < 2) {
            throw new IllegalArgumentException("SET command requires at least 2 arguments: key and value");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("SET command requires the key to be a bulk string");
        }

        // TODO: prepare for transaction support, currently we directly set the value in memory manager
        setInMemory(
                (String) keyRaw.getContent(),
                context.getArguments().get(1),
                context
        );

        // respond with OK
        context.getOutputStream().write(RedisSerializer.okString());
    }

    private void setInMemory(String key, RedisMessage value, CommandContext context) throws Exception {
        // check if expiration args are provided
        if (context.getArguments().size() < 4) {
            // no expiration args, set the value directly
            MemoryManager.set(key, value);
        } else {
            // expiration args provided
            RedisMessage expireTypeRaw = context.getArguments().get(2);
            RedisMessage expireValueRaw = context.getArguments().get(3);

            if (expireTypeRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING ||
                    expireValueRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("Expiration type and value must be bulk strings");
            }

            String expireValueStr = (String) expireValueRaw.getContent();
            long expireValue = Long.parseLong(expireValueStr);

            String expireType = (String) expireTypeRaw.getContent();
            Duration expireDuration = switch (expireType.toUpperCase()) {
                case "EX" -> Duration.ofSeconds(expireValue);
                case "PX" -> Duration.ofMillis(expireValue);
                default -> throw new IllegalArgumentException("Unsupported expiration type: " + expireType);
            };

            MemoryManager.set(key, value, expireDuration);
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "set".equalsIgnoreCase(commandName);
    }
}
