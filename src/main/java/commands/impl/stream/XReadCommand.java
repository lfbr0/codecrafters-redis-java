package commands.impl.stream;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.MemoryManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static serdes.RedisMessage.RedisMessageType.ARRAY;
import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class XReadCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            List<RedisMessage> args = context.getArguments();
            // Validate arguments
            if (args == null || args.isEmpty()) {
                throw new IllegalArgumentException("XREAD expects arguments");
            }

            Long blockTimeout = null;
            int streamsIdx = -1;

            for (int i = 0; i < args.size(); i++) {
                String arg = args.get(i).getContent().toString();
                if ("BLOCK".equalsIgnoreCase(arg)) {
                    if (i + 1 >= args.size()) {
                        throw new IllegalArgumentException("BLOCK requires a timeout argument");
                    }
                    blockTimeout = Long.parseLong(args.get(i + 1).getContent().toString());
                    i++;
                } else if ("STREAMS".equalsIgnoreCase(arg)) {
                    streamsIdx = i;
                    break;
                }
            }

            if (streamsIdx == -1 || streamsIdx + 1 >= args.size()) {
                throw new IllegalArgumentException("XREAD expects STREAMS <key> <id> [<key> <id> ...]");
            }

            // Parse keys and ids from arguments
            int numStreamPairs = (args.size() - (streamsIdx + 1)) / 2;
            List<String> keys = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            for (int i = 0; i < numStreamPairs; i++) {
                keys.add(args.get(streamsIdx + 1 + i).getContent().toString());
                ids.add(args.get(streamsIdx + 1 + numStreamPairs + i).getContent().toString());
            }

            // Handle the blocking logic if requested
            if (blockTimeout != null) {
                return handleBlockingRead(keys, ids, blockTimeout);
            }

            return handleNonBlockingRead(keys, ids);
        };
    }

    private CommandResponse handleNonBlockingRead(List<String> keys, List<String> ids) {
        List<RedisMessage> streamsResult = new ArrayList<>();

        // Fetch entries for each stream
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String id = ids.get(i);

            // XREAD is exclusive: only entries with ID > specified ID
            List<StreamEntry> entries = MemoryManager.rangeFromStream(key, id, null, false);
            if (entries.isEmpty()) {
                continue;
            }

            streamsResult.add(formatStreamResponse(key, entries));
        }

        // Return null array if no entries were found in any stream
        if (streamsResult.isEmpty()) {
            return new CommandResponse(RedisSerializer.nullArray());
        }

        // Return nested array of streams
        RedisMessage finalResponse = new RedisMessage().setType(ARRAY).setContent(streamsResult);
        return new CommandResponse(RedisSerializer.serialize(finalResponse));
    }

    private CommandResponse handleBlockingRead(List<String> keys, List<String> ids, long timeout) throws InterruptedException {
        // Resolve '$' IDs before potentially blocking
        List<String> resolvedIds = new ArrayList<>(ids);
        // resolution for '$' ID - should be last entry of stream at the time of the command
        for (int i = 0; i < keys.size(); i++) {
            if ("$".equals(ids.get(i))) {
                List<StreamEntry> allEntries = MemoryManager.rangeFromStream(keys.get(i), "0-0", null, true);
                if (!allEntries.isEmpty()) {
                    resolvedIds.set(i, allEntries.getLast().getStreamEntryId());
                } else {
                    resolvedIds.set(i, "0-0");
                }
            }
        }

        // Try non-blocking read first with resolved IDs
        CommandResponse immediateResponse = handleNonBlockingRead(keys, resolvedIds);
        String immediateResponseStr = new String(immediateResponse.getResponseBytes());
        if (!"*-1\r\n".equals(immediateResponseStr) && !"$-1\r\n".equals(immediateResponseStr)) {
            return immediateResponse;
        }

        // Nothing found, block on the first stream for simplicity (or all of them)
        SynchronousQueue<StreamEntry> queue = new SynchronousQueue<>();
        for (int i = 0; i < keys.size(); i++) {
            MemoryManager.blockingFromStream(keys.get(i), resolvedIds.get(i), queue);
        }

        StreamEntry newEntry = (timeout == 0) ? queue.take() : queue.poll(timeout, TimeUnit.MILLISECONDS);

        if (newEntry == null) {
            return new CommandResponse(RedisSerializer.nullArray());
        }

        // When a new entry arrives, we re-run non-blocking read to pick up all available data.
        return handleNonBlockingRead(keys, resolvedIds);
    }

    private RedisMessage formatStreamResponse(String key, List<StreamEntry> entries) {
        List<RedisMessage> entriesArray = new ArrayList<>();
        for (StreamEntry entry : entries) {
            RedisMessage entryId = new RedisMessage()
                    .setType(BULK_STRING)
                    .setContent(entry.getStreamEntryId());

            // Format entry properties as [key1, value1, key2, value2, ...]
            List<RedisMessage> properties = entry.getProperties()
                    .entrySet()
                    .stream()
                    .flatMap(prop -> Stream.of(
                            new RedisMessage().setType(BULK_STRING).setContent(prop.getKey()),
                            new RedisMessage().setType(BULK_STRING).setContent(prop.getValue())
                    ))
                    .toList();
            RedisMessage propertiesArray = new RedisMessage().setType(ARRAY).setContent(properties);

            // Add [id, properties] to entries array
            entriesArray.add(new RedisMessage()
                    .setType(ARRAY)
                    .setContent(List.of(entryId, propertiesArray)));
        }

        // [key, entries]
        return new RedisMessage()
                .setType(ARRAY)
                .setContent(List.of(
                        new RedisMessage().setType(BULK_STRING).setContent(key),
                        new RedisMessage().setType(ARRAY).setContent(entriesArray)
                ));
    }

    @Override
    public boolean matches(String commandName) {
        return "XREAD".equalsIgnoreCase(commandName);
    }
}
