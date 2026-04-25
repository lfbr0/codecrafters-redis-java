package handler;

import commands.Command;
import commands.CommandContext;
import commands.CommandResponse;
import commands.CommandRouter;
import data.AofPersistenceManager;
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

public class RedisClientHandler extends AbstractRedisClientHandler {

    private final Socket socket;
    private final AtomicBoolean shouldSendResponse = new AtomicBoolean(true);

    public RedisClientHandler(Socket socket) {
        this.socket = socket;
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

                CommandResponse commandResponse = handleMessage(message, socket.getOutputStream());
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

}
