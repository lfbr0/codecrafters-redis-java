package replication;

import logger.Logger;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.io.OutputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MasterReplicationTask extends Thread {

    private final UUID slaveId;
    private final OutputStream slaveOutputStream;
    private final BlockingQueue<RedisMessage> messageQueue;
    private final AtomicBoolean listening = new AtomicBoolean(false);

    public MasterReplicationTask(UUID slaveId, OutputStream slaveOutputStream) {
        super("RedisMasterReplicationThread");
        setDaemon(true); // this is an auxiliary thread, we don't want to block jvm kill
        this.slaveId = slaveId;
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

    public UUID getSlaveId() {
        return slaveId;
    }

    public boolean isListening() {
        return listening.get();
    }

    public void sendOffsetRequest() {
        try {
            Logger.info("\t\tSlave[" + slaveId + "] Sending offset request...");
            byte[] rawOffsetRequestMsg = RedisSerializer.listStrings(List.of("REPLCONF", "GETACK", "*"));
            slaveOutputStream.write(rawOffsetRequestMsg);
            slaveOutputStream.flush();
        } catch (Exception ex) {
            Logger.error("\t\tSlave[" + slaveId + "] Failed to send offset request: " + ex.getMessage(), ex);
        }
    }

}
