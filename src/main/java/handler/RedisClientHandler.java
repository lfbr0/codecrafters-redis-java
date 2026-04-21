package handler;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import commands.CommandRouter;
import logger.Logger;
import replication.ReplicationManager;
import serdes.RedisDeserializer;
import serdes.RedisMessage;

import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static serdes.RedisMessage.RedisMessageType.ARRAY;
import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class RedisClientHandler implements Runnable {

    private final Socket socket;
    private final AtomicBoolean inTransaction = new AtomicBoolean(false);
    private final AtomicReference<UUID> transactionId = new AtomicReference<>(null);
    private final AtomicBoolean shouldSendResponse = new AtomicBoolean(true);
    private final UUID clientUUID;

    public RedisClientHandler(Socket socket) {
        this.socket = socket;
        clientUUID = UUID.randomUUID();
    }

    public RedisClientHandler(Socket socket, boolean shouldSendResponse) {
        this(socket);
        this.shouldSendResponse.set(shouldSendResponse);
    }

    @Override
    public void run() {
        try {
            Logger.info("Starting RedisClientHandler for " + socket.getRemoteSocketAddress());
            while (socket.isConnected()) {
                RedisMessage message = RedisDeserializer.deserialize(socket.getInputStream());
                if (message == null) {
                    continue;
                }

                Logger.info("Received message: " + message);

                if (message.getType() != ARRAY) {
                    Logger.error("Expected an array message, but got: " + message.getType());
                    continue;
                }

                CommandResponse commandResponse = handleMessage(message);
                boolean sendCommandResponse = true;
                // reply to client if we have response and WE SHOULD RESPOND
                // if slave client, then no need to send response - we're joing doing our master's bidding
                // unless command explicitly overrides
                if (commandResponse == null) {
                    Logger.info("Will not respond back to client -> resp is null");
                    sendCommandResponse = false;
                } else if (!commandResponse.isSendEvenIfSlave() && !this.shouldSendResponse.get()) {
                    // if should not override and should not send
                    Logger.info("Will not respond back to client -> " +
                            "isSendEvenIfSlave=false && shouldSendResponse=false");
                    sendCommandResponse = false;
                }

                // if should send, then send it
                if (sendCommandResponse) {
                    // write to client response
                    Logger.info("Sending response to client: " + socket.getRemoteSocketAddress() + " - " + commandResponse);
                    socket.getOutputStream().write(commandResponse.getResponseBytes());

                    // execute post response callback - if it exists
                    CommandResponse.CommandPostResponseCallback postRespCb = commandResponse.getPostResponseCallback();
                    if (postRespCb != null) {
                        postRespCb.postResponse(socket.getOutputStream());
                    }
                }

                // increment bytes received if slave
                if (!ReplicationManager.isMaster()) {
                    Logger.info("Received bytes as slaved incremented, now is: " +
                            ReplicationManager.addMasterReplOffsetBytes(message.getContentBytes().length)
                    );
                }
            }
        } catch (Exception e) {
            Logger.error("Failed to interpret command: " + e.getMessage(), e);
        }
    }

    /**
     * Handles the incoming Redis message and executes the corresponding command.
     * @param message array message containing the command and its arguments
     */
    private CommandResponse handleMessage(RedisMessage message) throws Exception {
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
                socket.getOutputStream(),
                elements.stream().skip(1).toList() // ignore command name, pass only arguments
        );

        if (inTransaction.get()) {
            Logger.info("Received a command while in transaction mode. Commands will be queued until MULTI is executed.");
            ctx.startTransaction(transactionId.get());
        }

        Logger.info("Context info: " + ctx);
        Command command = CommandRouter.getCommand((String) elements.getFirst().getContent());

        // replication - if write command, replicate to slaves if master
        if (command.isWriteCommand() && ReplicationManager.isMaster()) {
            Logger.info("Command " + command + " is write, so replicating message=" + message);
            ReplicationManager.replicate(message);
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
            Logger.info("Entering transaction mode for client: " + socket.getRemoteSocketAddress() +
                    " with transaction ID: " + ctx.getTransactionId());
            inTransaction.set(true);
            transactionId.set(ctx.getTransactionId());

        } else if (inTransaction.get() && !ctx.isInTransaction()) {
            // transaction ended, reset flag & id - because received ctx has transaction=false but client is transaction=true
            Logger.info("Exiting transaction mode for client: " + socket.getRemoteSocketAddress()
                    + " with transaction ID: " + transactionId.get());
            inTransaction.set(false);
            transactionId.set(null);
        }

        return commandResponse;
    }
}
