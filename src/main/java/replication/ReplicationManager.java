package replication;

import logger.Logger;
import serdes.RedisMessage;

import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ReplicationManager {

    private static final long REPLICATION_TIMEOUT_MINUTES = 3;

    // state vars
    private static final AtomicBoolean isMaster = new AtomicBoolean(true);
    private static final AtomicReference<String> masterReplId = new AtomicReference<>(generateMasterReplicationId());
    private static final AtomicLong masterReplOffset = new AtomicLong(0);

    private static final ExecutorService virtualThreadPool = Executors.newVirtualThreadPerTaskExecutor();
    private static final List<MasterReplicationTask> replicationMasterTasks = Collections.synchronizedList(new ArrayList<>());
    private static final Map<UUID, BlockingQueue<Long>> slaveAcknowledgementQueues = new ConcurrentHashMap<>();

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
        BlockingQueue<String> slaveReplicationIdRespQueue = new LinkedBlockingQueue<>();
        // replication thread to do it concurrently
        SlaveReplicationThread slaveReplicationThread =
                new SlaveReplicationThread(masterHost, masterPort, myPort, slaveReplicationIdRespQueue);
        slaveReplicationThread.start();
        // wait for it to finish & gather master info
        try {
            String masterReplicationId = slaveReplicationIdRespQueue.poll(REPLICATION_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            Logger.info("SETTING NEW MASTER REPLICATION ID: " + masterReplicationId);
            masterReplId.set(masterReplicationId);
            // start handler for client
            slaveReplicationThread.join();
            Runnable slaveRunnable = slaveReplicationThread.getSlaveReplicationHandler();
            if (slaveRunnable != null) {
                Logger.info("Starting runnable for Slave Listener...");
                virtualThreadPool.submit(slaveRunnable);
            }
        } catch (InterruptedException e) {
            Logger.error("Replication thread was interrupted!", e);
        }
    }

    /**
     * Starts replication to slave port as master
     *
     * @param clientUUID
     * @param slaveOutputStream slave to send info to
     */
    public static void replicateTo(UUID clientUUID, OutputStream slaveOutputStream) {
        MasterReplicationTask task = new MasterReplicationTask(clientUUID, slaveOutputStream);
        virtualThreadPool.submit(task);
        replicationMasterTasks.add(task);
        slaveAcknowledgementQueues.put(clientUUID, new LinkedBlockingQueue<>());
    }

    public static void replicate(RedisMessage message) {
        // increment number of master bytes
        masterReplOffset.addAndGet(message.getContentBytes().length);
        // replicate to slaves
        replicationMasterTasks.forEach(t -> t.replicate(message));
    }

    public static boolean setReplicaAcknowledge(UUID clientUUID, long bytes) {
        Logger.info("Acknowledging for slave " + clientUUID + " received bytes: " + bytes);
        return slaveAcknowledgementQueues.get(clientUUID).offer(bytes);
    }

    public static long getReplicaCount(long timeout) {
        List<MasterReplicationTask> replicas = new ArrayList<>(replicationMasterTasks);
        List<CompletableFuture<Boolean>> syncedReplicaCfs = new ArrayList<>(replicas.size());
        long sentBytesToSlaves = getMasterReplOffset();
        AtomicLong syncedUpReplicas = new AtomicLong(0);

        for (MasterReplicationTask masterReplicationTask : replicas) {
            if (!masterReplicationTask.isListening()) {
                Logger.info("Skipping check for slave [" + masterReplicationTask.getSlaveId() + "], not listen!");
                continue;
            }

            // sent no bytes, so we just want to know if is listening
            if (sentBytesToSlaves == 0) {
                syncedUpReplicas.incrementAndGet();
                continue;
            }

            // job to get listened to bytes (down the future)
            syncedReplicaCfs.add(CompletableFuture.supplyAsync(() -> {
                // send request
                masterReplicationTask.sendOffsetRequest();
                // wait for received bytes
                Logger.info("Slave[" + masterReplicationTask.getSlaveId() + "] waiting for received bytes...");
                try {
                    Long receivedBytes = slaveAcknowledgementQueues
                            .get(masterReplicationTask.getSlaveId())
                            .poll(timeout, TimeUnit.MILLISECONDS);
                    Logger.info("Received for slave[" + masterReplicationTask.getSlaveId() + "] bytes: " + receivedBytes);

                    // return if it matches sent bytes
                    if (receivedBytes != null && receivedBytes >= sentBytesToSlaves) {
                        syncedUpReplicas.incrementAndGet();
                        return true;
                    }
                } catch (Exception ex) {
                    Logger.error("Failed to receive bytes from slave[" + masterReplicationTask.getSlaveId() + "] " +
                            "-> " + ex.getMessage(), ex);
                }
                return false;
            }, virtualThreadPool));
        }

        try {
            // wait for all with timeout
            CompletableFuture
                    .allOf(syncedReplicaCfs.toArray(CompletableFuture[]::new))
                    .get(timeout, TimeUnit.SECONDS);
        } catch (Exception ex) {
            Logger.error("Failed to get for replicas ack -> " + ex.getMessage(), ex);
        }

        return syncedUpReplicas.get();
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

    public static boolean isMaster() { return isMaster.get(); }

    public static long getMasterReplOffset() { return masterReplOffset.get(); }

    public static long addMasterReplOffsetBytes(int bytes) {
        return masterReplOffset.addAndGet(bytes);
    }
}
