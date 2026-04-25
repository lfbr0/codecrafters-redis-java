package handler;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import commands.CommandRouter;
import data.AofPersistenceManager;
import logger.Logger;
import pubsub.PubSubManager;
import replication.ReplicationManager;
import serdes.RedisMessage;

import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public abstract class AbstractRedisClientHandler implements Runnable {

    private static final String SUBSCRIBER_MODE_ERROR = "ERR Can't execute 'echo': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context";

    private final UUID clientUUID = UUID.randomUUID();
    private final AtomicBoolean inTransaction = new AtomicBoolean(false);
    private final AtomicReference<UUID> transactionId = new AtomicReference<>(null);

    /**
     * Handles the incoming Redis message and executes the corresponding command.
     * @param message array message containing the command and its arguments
     */
    protected CommandResponse handleMessage(RedisMessage message, OutputStream outputStream) throws Exception {
        if (message.getContent() == null || !(message.getContent() instanceof List)) {
            Logger.error("Invalid message content: expected a list of RedisMessage, but got: " + message.getContent());
            return null;
        }

        List<RedisMessage> elements = (List<RedisMessage>) message.getContent();
        if (elements.isEmpty()) {
            Logger.error("Received an empty command array.");
            return null;
        }

        // match to commands
        if (elements.getFirst().getType() != BULK_STRING) {
            Logger.error("Expected the first element to be a bulk string command, but got: " + elements.getFirst().getType());
            return null;
        }

        CommandContext ctx = new CommandContext(
                clientUUID,
                outputStream,
                elements.stream().skip(1).toList() // ignore command name, pass only arguments
        );

        if (inTransaction.get()) {
            Logger.info("Received a command while in transaction mode. Commands will be queued until MULTI is executed.");
            ctx.startTransaction(transactionId.get());
        }

        Logger.info("Context info: " + ctx);
        Command command = CommandRouter.getCommand((String) elements.getFirst().getContent());

        // if in subcriber mode, cannot execute non pub/sub commands
        if (!command.isSubscriberModeAllowedCommand() &&
            !PubSubManager.getInstance().getClientSubscriptions(clientUUID).isEmpty()) {
            return CommandResponse.error(SUBSCRIBER_MODE_ERROR);
        }

        // replication - if write command, replicate to slaves if master
        if (command.isWriteCommand() && ReplicationManager.isMaster()) {
            Logger.info("Command " + command + " is write, so replicating message=" + message);
            ReplicationManager.replicate(message);
        }

        // aof persistence
        if (command.isWriteCommand() && AofPersistenceManager.getInstance().isEnabled()) {
            Logger.info("Command " + command +
                    " is write, so persisting message=" + message +
                    " into AOF=" + AofPersistenceManager.getInstance().persist(message));
        }

        CommandResponse commandResponse;
        try {
            commandResponse = command.execute(ctx);
        } catch (IllegalArgumentException | IllegalStateException ex) {
            Logger.error("Error occured in parsing command response -> " + ex.getMessage(), ex);
            commandResponse = CommandResponse.error(ex.getMessage());
        }
        Logger.info("Command response: " + commandResponse);

        // if entered transaction mode, set flag & id
        if (!inTransaction.get() && ctx.isInTransaction()) {
            Logger.info("Entering transaction mode for client: " + clientUUID +
                    " with transaction ID: " + ctx.getTransactionId());
            inTransaction.set(true);
            transactionId.set(ctx.getTransactionId());

        } else if (inTransaction.get() && !ctx.isInTransaction()) {
            // transaction ended, reset flag & id - because received ctx has transaction=false but client is transaction=true
            Logger.info("Exiting transaction mode for client: " + clientUUID +
                    " with transaction ID: " + transactionId.get());
            inTransaction.set(false);
            transactionId.set(null);
        }

        return commandResponse;
    }

}
