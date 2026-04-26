package commands.impl.stream;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import logger.Logger;
import serdes.RedisMessage;

import java.util.concurrent.Callable;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class XAddCommand implements Command {

    public static final String ERROR_ZERO_STREAM_ENTRY_ID = "ERR The ID specified in XADD must be greater than 0-0";
    public static final String ERROR_OLD_STREAM_ENTRY_ID = "ERR The ID specified in XADD is equal or " +
            "smaller than the target stream top item";

    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() < 4) {
                throw new IllegalArgumentException("XADD expects at least (stream key, id, arg1, arg_value1)");
            }

            RedisMessage streamKeyRaw = context.getArguments().getFirst();
            RedisMessage streamEntryIdRaw = context.getArguments().get(1);
            if (streamKeyRaw.getType() != streamEntryIdRaw.getType() || streamEntryIdRaw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("XADD stream key and entry id must be BULK STRING!");
            }

            String streamKey = streamKeyRaw.getContent().toString();
            String streamEntryId = streamEntryIdRaw.getContent().toString();
            if (streamEntryId.equals("0-0")) {
                return CommandResponse.error(ERROR_ZERO_STREAM_ENTRY_ID);
            }
            StreamEntry streamEntry = new StreamEntry(streamKey, streamEntryId);

            for (int i = 2; i < context.getArguments().size() - 1; ++i) {
                streamEntry.addProperty(
                        context.getArguments().get(i).getContent().toString(),
                        context.getArguments().get(i + 1).getContent().toString()
                );
            }

            // if did not add to stream, it was because the key is older than any entry in stream
            return MemoryManager
                    .addToStream(streamKey, streamEntry)
                    .map(CommandResponse::bulkString)
                    .orElse(CommandResponse.error(ERROR_OLD_STREAM_ENTRY_ID));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "XADD".equalsIgnoreCase(commandName);
    }

    @Override
    public boolean isWriteCommand() {
        return true;
    }
}
