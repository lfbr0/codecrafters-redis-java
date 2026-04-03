package replication;

import logger.Logger;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

public class ReplicationMasterThread extends Thread {

    private final int port;
    private final Queue<RedisMessage> messageQueue;

    public ReplicationMasterThread(int port) {
        super("RedisMasterReplicationThread");
        setDaemon(true); // this is an auxiliary thread, we don't want to block jvm kill
        this.port = port;
        this.messageQueue = new SynchronousQueue<>();
    }

    public void replicate(RedisMessage message) {
        this.messageQueue.offer(message);
    }

    @Override
    public void run() {
        try (Socket socket = new Socket("localhost", port)) {
            while (true) {
                RedisMessage redisMessage = this.messageQueue.poll();
                if (redisMessage != null) {
                    socket.getOutputStream().write(RedisSerializer.serialize(redisMessage));
                    socket.getOutputStream().flush();
                }
            }
        } catch (Exception ex) {
            Logger.error("Failed to replicate to slaves: " + ex.getMessage(), ex);
        }
    }
}
