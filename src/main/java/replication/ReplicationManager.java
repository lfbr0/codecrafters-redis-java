package replication;

import logger.Logger;
import serdes.RedisMessage;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicationManager {

    // state vars
    private static final AtomicBoolean isMaster = new AtomicBoolean(true);
    private static final AtomicReference<String> masterReplId = new AtomicReference<>(generateMasterReplicationId());
    private static final AtomicLong masterReplOffset = new AtomicLong(0);

    // replication thread to do it concurrently
    private static SlaveReplicationThread slaveReplicationThread;
    private static final ExecutorService replicationMasterThreadPool = Executors.newVirtualThreadPerTaskExecutor();
    private static final List<MasterReplicationTask> replicationMasterTasks = Collections.synchronizedList(new ArrayList<>());


    /**
     * Generates master replication id
     * @return A 40-character alphanumeric string
     */
    private static String generateMasterReplicationId() {
        return ("redis000" + UUID.randomUUID().toString().replaceAll("-",""));
    }

    /**
     * Starts replication process given master host & port as slave
     * @param masterHost master host
     * @param masterPort master port
     */
    public static void replicateFrom(String masterHost, int masterPort, int myPort) {
        // mark as slave - master by default
        isMaster.set(false);
        // start replication thread
        slaveReplicationThread = new SlaveReplicationThread(masterHost, masterPort, myPort);
        slaveReplicationThread.start();
        // wait for it to finish & gather master info
        try {
            slaveReplicationThread.join();
            Logger.info("SETTING NEW MASTER REPLICATION ID: " + slaveReplicationThread.getMasterReplicationId());
            masterReplId.set(slaveReplicationThread.getMasterReplicationId());
        } catch (InterruptedException e) {
            Logger.error("Replication thread was interrupted!", e);
        }
    }

    /**
     * Starts replication to slave port as master
     * @param slaveOutputStream slave to send info to
     */
    public static void replicateTo(OutputStream slaveOutputStream) {
        MasterReplicationTask task = new MasterReplicationTask(slaveOutputStream);
        replicationMasterThreadPool.submit(task);
        replicationMasterTasks.add(task);
    }

    public static void replicate(RedisMessage message) {
        replicationMasterTasks.forEach(t -> t.replicate(message));
    }

    /**
     * Returns all server info as it stands...
     * @return all info
     */
    public static String getInfo() {
        return new StringBuilder()
                // replication
                .append("# Replication").append("\n")
                .append("role:").append(isMaster.get() ? "master" : "slave").append("\n")
                .append("master_replid:").append(masterReplId).append("\n")
                .append("master_repl_offset:").append(masterReplOffset.get())
                .toString();
    }

    public static String getReplicationId() {
        return masterReplId.get();
    }
}
