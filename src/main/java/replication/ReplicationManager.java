package replication;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicationManager {

    private static final AtomicBoolean isMaster = new AtomicBoolean(true);

    public static String getInfo() {
        return new StringBuilder()
                // replication
                .append("# Replication").append("\n")
                .append("role:").append(isMaster.get() ? "master" : "slave")
                .toString();
    }

}
