import commands.CommandContext;
import commands.CommandRouter;
import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;

import java.net.Socket;
import java.util.List;

import static serdes.RedisMessage.RedisMessageType.ARRAY;
import static serdes.RedisMessage.RedisMessageType.BULK_STRING;

public class RedisClientHandler implements Runnable {

    private final Socket socket;

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
        Logger.info("Context info: " + ctx);

        CommandRouter
                .getCommand((String) elements.getFirst().getContent())
                .execute(ctx);
    }
}
