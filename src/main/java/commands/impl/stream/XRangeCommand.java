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

public class XRangeCommand implements Command {
    @Override
    public Callable<CommandResponse> handleContext(CommandContext context) {
        return () -> {
            if (context.getArguments() == null || context.getArguments().size() != 3) {
                throw new IllegalArgumentException("XRANGE expects 3 arguments (stream key, start ts, end ts)!");
            }

            RedisMessage streamKeyRaw = context.getArguments().getFirst(),
                    startEntryIdRaw = context.getArguments().get(1),
                    endEntryIdRaw = context.getArguments().getLast();
            if (streamKeyRaw.getType() != startEntryIdRaw.getType() ||
                streamKeyRaw.getType() != endEntryIdRaw.getType() ||
                streamKeyRaw.getType() != BULK_STRING) {
                throw new IllegalArgumentException("XRANGE expects all arguments to be of type BULK STRING!");
            }

            String streamKey = streamKeyRaw.getContent().toString(),
                    startEntryId = startEntryIdRaw.getContent().toString(),
                    endEntryId = endEntryIdRaw.getContent().toString();

            // format entry ids
            startEntryId = formatEntryId(startEntryId);
            endEntryId = formatEntryId(endEntryId);
            if (startEntryId == null || endEntryId == null) {
                throw new IllegalArgumentException("XRANGE failed to format start entry id or end entry id!");
            }

            List<RedisMessage> innerStreamEntriesArray = MemoryManager
                    .rangeFromStream(streamKey, startEntryId, endEntryId)
                    .stream()
                    .map(streamEntry -> {
                        RedisMessage entryId = new RedisMessage()
                                .setType(BULK_STRING)
                                .setContent(streamEntry.getStreamEntryId());

                        // take care of properties now
                        List<RedisMessage> properties = streamEntry.getProperties()
                                .entrySet()
                                .stream()
                                .flatMap(entry -> Stream.of(
                                        new RedisMessage().setType(BULK_STRING).setContent(entry.getKey()),
                                        new RedisMessage().setContent(BULK_STRING).setContent(entry.getValue())
                                ))
                                .toList();
                        RedisMessage propertiesArray = new RedisMessage().setType(ARRAY).setContent(properties);

                        // final array with this entry
                        return new RedisMessage()
                                .setType(ARRAY)
                                .setContent(List.of(entryId, propertiesArray));
                    })
                    .toList();

            // wrap all of this up into a final array
            RedisMessage finalArray = new RedisMessage().setType(ARRAY).setContent(innerStreamEntriesArray);
            return new CommandResponse(RedisSerializer.serialize(finalArray));
        };
    }

    private String formatEntryId(String entryId) {
        // no need for replacement
        if (entryId.matches("[0-9]+-[0-9]+")) {
            return entryId;
        }
        // make sequence be starting at 0
        if (entryId.matches("[0-9]+-\\*")) {
            return entryId.replaceAll("\\*", "0");
        }
        // return baseline entry id
        if (entryId.matches("\\*")) {
            return "0-0";
        }
        // not a valid entry id
        return null;
    }

    @Override
    public boolean matches(String commandName) {
        return false;
    }
}
