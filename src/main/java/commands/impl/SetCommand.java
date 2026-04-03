package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.time.Duration;
import java.util.concurrent.Callable;

public class SetCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() < 2) {
            throw new IllegalArgumentException("SET command requires at least 2 arguments: key and value");
        }

        RedisMessage keyRaw = context.getArguments().getFirst();
        if (keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("SET command requires the key to be a bulk string");
        }

        Callable<byte[]> task = () -> {
            setInMemory(
                    (String) keyRaw.getContent(),
                    context.getArguments().get(1),
                    context
            );
            return RedisSerializer.okString();
        };

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), task);
            return CommandResponse.queued();
        }

        return new CommandResponse(task.call());
    }

    private void setInMemory(String key, RedisMessage value, CommandContext context) throws Exception {
        // check if expiration args are provided
        if (context.getArguments().size() < 4) {
            // no expiration args, set the value directly
            MemoryManager.set(key, value);
        } else {
            // expiration args provided
            RedisMessage expireTypeRaw = context.getArguments().get(2);
            if (expireTypeRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("Expiration type must be a bulk string");
            }

            RedisMessage expireValueRaw = context.getArguments().get(3);
            if (expireValueRaw.getType() != RedisMessage.RedisMessageType.INTEGER) {
                throw new IllegalArgumentException("Expiration value must be an integer");
            }

            String expireType = (String) expireTypeRaw.getContent();
            int expireValue = (Integer) expireValueRaw.getContent();

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
