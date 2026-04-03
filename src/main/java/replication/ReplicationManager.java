package replication;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicationManager {

    // state vars
    private static final AtomicBoolean isMaster = new AtomicBoolean(true);

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
                .append("role:").append(isMaster.get() ? "master" : "slave")
                .toString();
    }

}
