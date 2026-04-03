import commands.CommandContext;
import commands.CommandResponse;
import commands.CommandRouter;
import data.TransactionManager;
import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static serdes.RedisMessage.RedisMessageType.ARRAY;
import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class RedisClientHandler implements Runnable {

    private final Socket socket;
    private final AtomicBoolean inTransaction = new AtomicBoolean(false);
    private final AtomicReference<UUID> transationId = new AtomicReference<>(null);

    public RedisClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            while (socket.isConnected()) {
                RedisMessage message = RedisDeserializer.deserialize(socket.getInputStream());
                if (message == null) {
                    Logger.error("Received null message from client. Closing connection.");
                    socket.close();
                    return;
                }

                Logger.info("Received message: " + message);

                if (message.getType() != ARRAY) {
                    Logger.error("Expected an array message, but got: " + message.getType());
                    continue;
                }

                handleMessage(message);
            }
        } catch (Exception e) {
            Logger.error("Failed to interpret command: " + e.getMessage(), e);
        }
    }

    /**
     * Handles the incoming Redis message and executes the corresponding command.
     * @param message array message containing the command and its arguments
     */
    private void handleMessage(RedisMessage message) throws Exception {
        if (message.getContent() == null || !(message.getContent() instanceof List)) {
            Logger.error("Invalid message content: expected a list of RedisMessage, but got: " + message.getContent());
            return;
        }

        List<RedisMessage> elements = (List<RedisMessage>) message.getContent();
        if (elements.isEmpty()) {
            Logger.error("Received an empty command array.");
            return;
        }

        // match to commands
        if (elements.getFirst().getType() != BULK_STRING) {
            Logger.error("Expected the first element to be a bulk string command, but got: " + elements.getFirst().getType());
            return;
        }

        CommandContext ctx = new CommandContext(
                socket.getOutputStream(),
                elements.stream().skip(1).toList() // ignore command name, pass only arguments
        );

        if (inTransaction.get()) {
            Logger.info("Received a command while in transaction mode. Commands will be queued until MULTI is executed.");
            ctx.startTransaction(transationId.get());
        }

        Logger.info("Context info: " + ctx);
        CommandResponse commandResponse = CommandRouter
                .getCommand((String) elements.getFirst().getContent())
                .execute(ctx);

        // if entered transaction mode, set flag & id
        if (!inTransaction.get() && ctx.isInTransaction()) {
            Logger.info("Entering transaction mode for client: " + socket.getRemoteSocketAddress() +
                    " with transaction ID: " + ctx.getTransactionId());
            inTransaction.set(true);
            transationId.set(ctx.getTransactionId());

        } else if (inTransaction.get() && !ctx.isInTransaction()) {
            // transaction ended, reset flag & id - because received ctx has transaction=false but client is transaction=true
            Logger.info("Exiting transaction mode for client: " + socket.getRemoteSocketAddress()
                    + " with transaction ID: " + transationId.get());
            inTransaction.set(false);
            transationId.set(null);
        }

        // write to client response
        Logger.info("Sending response to client: " + socket.getRemoteSocketAddress() + " - " + commandResponse);
        socket.getOutputStream().write(commandResponse.getResponseBytes());
    }
}
