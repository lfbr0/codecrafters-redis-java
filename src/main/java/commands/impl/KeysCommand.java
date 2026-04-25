package commands.impl;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import data.RdbPersistenceManager;
import data.TransactionManager;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.util.List;
import java.util.concurrent.Callable;

public class KeysCommand implements Command {
    @Override
    public CommandResponse execute(CommandContext context) throws Exception {
        if (context.getArguments() == null || context.getArguments().size() != 1) {
            throw new IllegalArgumentException("KEYS expects one argument with filter!");
        }

        RedisMessage filterRaw = context.getArguments().getFirst();
        if (filterRaw.getType() != RedisMessage.RedisMessageType.BULK_STRING) {
            throw new IllegalArgumentException("KEYS expects filter to be a bulk string!");
        }

        String filter = filterRaw.getContent().toString();
        Callable<CommandResponse> operation = () -> {
            String filterAsRegex = filter.replace("*", ".*");
            List<String> matchingKeys = RdbPersistenceManager.getInstance()
                    .orElseThrow(() -> new IllegalArgumentException("KEYS cannot be performed since Persistence Manager is null!"))
                    .getPersistedKeys()
                    .stream()
                    .filter(key -> key.matches(filterAsRegex))
                    .toList();

            return new CommandResponse(RedisSerializer.listStrings(matchingKeys));
        };

        if (context.isInTransaction()) {
            TransactionManager.addOperation(context.getTransactionId(), () -> operation.call().getResponseBytes());
            return CommandResponse.queued();
        }

        return operation.call();
    }

    @Override
    public boolean matches(String commandName) {
        return "KEYS".equalsIgnoreCase(commandName);
    }
}
