package replication;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationManager {

    // state vars
    private static final AtomicBoolean isMaster = new AtomicBoolean(true);
    private static final String masterReplId = generateMasterReplicationId();
    private static final AtomicLong masterReplOffset = new AtomicLong(0);

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
    public static void replicateFrom(String masterHost, int masterPort) {
        // mark as slave - master by default
        isMaster.set(false);
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
