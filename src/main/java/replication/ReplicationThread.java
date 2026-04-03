package replication;

import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.net.Socket;
import java.util.List;

public class ReplicationThread extends Thread {

    private final String masterHost;
    private final int masterPort;

    public ReplicationThread(String masterHost, int masterPort) {
        super("RedisSlaveReplicationThread");
        setDaemon(true); // this is an auxiliary thread, we don't want to block jvm kill
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    @Override
    public void run() {
        try {
            Logger.info("Starting replication from master at host=" + masterHost + ", port=" + masterPort);
            Socket socket = new Socket(masterHost, masterPort);

            // phase 1 - send PING as RESP array & expect PONG back
            RedisMessage pingMsg = new RedisMessage();
            pingMsg.setContent("PING");
            pingMsg.setType(RedisMessage.RedisMessageType.BULK_STRING);
            // send message bytes
            socket.getOutputStream().write(RedisSerializer.list(List.of(pingMsg)));
            // read message back
            RedisMessage pongMsg = RedisDeserializer.deserialize(socket.getInputStream().readAllBytes());
            Logger.info("Received message from master: " + pongMsg);
            assert pongMsg != null;
            assert pongMsg.getType() == RedisMessage.RedisMessageType.BULK_STRING;
            assert pongMsg.getContent().toString().equalsIgnoreCase("PONG");

        } catch (Exception ex) {
            Logger.error("Failed to replicate: " + ex.getMessage(), ex);
        }
    }
}
