package commands.impl;

import commands.Command;
import commands.CommandContext;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class BlpopCommand implements Command {
    @Override
    public void execute(CommandContext context) throws Exception {
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

        // TODO: adapt to transaction
        SynchronousQueue<RedisMessage> queue = new SynchronousQueue<>();
        MemoryManager.blockingPopFromList(key, queue);
        RedisMessage poppedValue = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        
        if (poppedValue == null) {
            context.getOutputStream().write(RedisSerializer.nullArray());
        } else {
            RedisMessage keyMessage = RedisSerializer.bulkString(key);
            context.getOutputStream().write(RedisSerializer.list(List.of(keyMessage, poppedValue)));
        }
    }

    @Override
    public boolean matches(String commandName) {
        return "blpop".equalsIgnoreCase(commandName);
    }
}
