package commands.impl.list;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlpopCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() < 2) {
                throw new IllegalArgumentException("BLPOP command requires at least 2 arguments: key and timeout");
            }

            RedisMessage keyRaw = context.getArguments().get(0);
            RedisMessage timeoutRaw = context.getArguments().get(1);
            if (keyRaw.getType() != timeoutRaw.getType() && keyRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
                throw new IllegalArgumentException("BLPOP command arguments must be bulk strings");
            }

            String key = (String) keyRaw.getContent();
            float timeout = Float.parseFloat((String) timeoutRaw.getContent());
            if (timeout == 0) {
                timeout = Float.MAX_VALUE; // block indefinitely
            }
            long timeoutMillis = (long) (timeout * 1000);

            return new CommandResponse(performBlockingPop(key, timeoutMillis));
        };
    }

    private byte[] performBlockingPop(String key, long timeoutMillis) throws InterruptedException {
        BlockingQueue<RedisMessage> queue = new LinkedBlockingQueue<>(1);
        MemoryManager.blockingPopFromList(key, queue);
        RedisMessage poppedValue = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);

        if (poppedValue == null) {
            return RedisSerializer.nullArray();
        } else {
            RedisMessage keyMessage = new RedisMessage();
            keyMessage.setType(RedisMessage.RedisMessageType.BULK_STRING);
            keyMessage.setContent(key);
            // write response as an array of [key, poppedValue]
            return RedisSerializer.list(List.of(keyMessage, poppedValue));
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "blpop".equalsIgnoreCase(commandName);
    }
}
