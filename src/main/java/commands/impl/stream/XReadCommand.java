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
import java.util.stream.Stream;

import static serdes.RedisMessage.RedisMessageType.ARRAY;
import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class XReadCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            List<RedisMessage> args = context.getArguments();
            // Validate arguments: XREAD STREAMS <key> <id> [<key> <id> ...]
            if (args == null || args.size() < 3 || !"STREAMS".equalsIgnoreCase(args.getFirst().getContent().toString())) {
                throw new IllegalArgumentException("XREAD expects STREAMS <key> <id> [<key> <id> ...]");
            }

            // Parse keys and ids from arguments
            int numStreamPairs = (args.size() - 1) / 2;
            List<String> keys = new ArrayList<>();
            List<String> ids = new ArrayList<>();

            for (int i = 0; i < numStreamPairs; i++) {
                keys.add(args.get(1 + i).getContent().toString());
                ids.add(args.get(1 + numStreamPairs + i).getContent().toString());
            }

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

                // Add [key, entries] to streams result array
                streamsResult.add(new RedisMessage()
                        .setType(ARRAY)
                        .setContent(List.of(
                                new RedisMessage().setType(BULK_STRING).setContent(key),
                                new RedisMessage().setType(ARRAY).setContent(entriesArray)
                        )));
            }

            // Return null bulk string if no entries were found in any stream
            if (streamsResult.isEmpty()) {
                return new CommandResponse(RedisSerializer.nullBulkString());
            }

            // Return nested array of streams
            RedisMessage finalResponse = new RedisMessage().setType(ARRAY).setContent(streamsResult);
            return new CommandResponse(RedisSerializer.serialize(finalResponse));
        };
    }

    @Override
    public boolean matches(String commandName) {
        return "XREAD".equalsIgnoreCase(commandName);
    }
}
