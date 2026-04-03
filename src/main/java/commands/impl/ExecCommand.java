package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.TransactionManager;
import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;
import static serdes.RedisMessage.RedisMessageType.INTEGER;

public class ExecCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (!context.isInTransaction()) {
            return new CommandResponse(RedisSerializer.error("ERR EXEC without MULTI"));
        }

        Logger.info("Exiting transaction mode " + context.getTransactionId());
        UUID transactionId = context.endTransaction();

        // execute all transactions
        List<byte[]> messagesRaw = TransactionManager.commitTransaction(transactionId);
        List<RedisMessage> messages = new ArrayList<>(messagesRaw.size());
        for (byte[] messageRaw : messagesRaw) {
            RedisMessage message = RedisDeserializer.deserialize(messageRaw);

            // convert integers to bulk strings because that's how Redis does it
            if (message.getType() == INTEGER) {
                message.setType(BULK_STRING);

                String content;
                if (message.getContent() instanceof Integer) {
                    int value = (Integer) message.getContent();
                    content = Integer.toString(value);
                } else {
                    content = message.getContent().toString();
                }

                message.setContent(content.getBytes());
            }

            messages.add(message);
        }

        return new CommandResponse(RedisSerializer.list(messages));
    }

    @Override
    public boolean matches(String commandName) {
        return "exec".equalsIgnoreCase(commandName);
    }
}
