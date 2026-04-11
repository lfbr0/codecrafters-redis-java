package replication;

import logger.Logger;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.io.OutputStream;
import java.net.Socket;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MasterReplicationTask extends Thread {

    private final UUID slaveId = UUID.randomUUID();
    private final OutputStream slaveOutputStream;
    private final BlockingQueue<RedisMessage> messageQueue;
    private final AtomicBoolean listening = new AtomicBoolean(false);

    public MasterReplicationTask(OutputStream slaveOutputStream) {
        super("RedisMasterReplicationThread");
        setDaemon(true); // this is an auxiliary thread, we don't want to block jvm kill
        this.slaveOutputStream = slaveOutputStream;
        this.messageQueue = new LinkedBlockingQueue<>();
    }

    public void replicate(RedisMessage message) {
        Logger.info("\t\tAdding to slave[" + slaveId + "] message=" + message);
        this.messageQueue.offer(message);
    }

    @Override
    public void run() {
        try {
            Logger.info("\t\tSlave[" + slaveId + "] ready to listen!");
            listening.set(true);

            while (true) {
                RedisMessage redisMessage = this.messageQueue.take();
                Logger.info("\t\tSlave[" + slaveId + "] fetched message=" + redisMessage);
                slaveOutputStream.write(redisMessage.getContentBytes());
                slaveOutputStream.flush();
            }
        } catch (Exception ex) {
            Logger.error("\t\tFailed to replicate to slaves: " + ex.getMessage(), ex);
        } finally {
            Logger.info("\t\tClosing slave[" + slaveId + "]");
            listening.set(false);
        }
    }

    public boolean isListening() {
        return listening.get();
    }
}
