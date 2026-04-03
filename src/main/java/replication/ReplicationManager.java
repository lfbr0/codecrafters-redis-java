package replication;

import logger.Logger;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicationManager {

    // state vars
    private static final AtomicBoolean isMaster = new AtomicBoolean(true);
    private static final AtomicReference<String> masterReplId = new AtomicReference<>(generateMasterReplicationId());
    private static final AtomicLong masterReplOffset = new AtomicLong(0);

    // replication thread to do it concurrently
    private static ReplicationThread replicationThread;


    /**
     * Generates master replication id
     * @return A 40-character alphanumeric string
     */
    private static String generateMasterReplicationId() {
        return ("redis000" + UUID.randomUUID().toString().replaceAll("-",""));
    }

    /**
     * Starts replication process given master host & port
     * @param masterHost master host
     * @param masterPort master port
     */
    public static void replicateFrom(String masterHost, int masterPort, int myPort) {
        // mark as slave - master by default
        isMaster.set(false);
        // start replication thread
        replicationThread = new ReplicationThread(masterHost, masterPort, myPort);
        replicationThread.start();
        // wait for it to finish & gather master info
        try {
            replicationThread.join();
            Logger.info("SETTING NEW MASTER REPLICATION ID: " + replicationThread.getMasterReplicationId());
            masterReplId.set(replicationThread.getMasterReplicationId());
        } catch (InterruptedException e) {
            Logger.error("Replication thread was interrupted!", e);
        }
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

}
