import data.AofPersistenceManager;
import data.RdbPersistenceManager;
import handler.RedisClientHandler;
import logger.Logger;
import replication.ReplicationManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class RedisServer implements AutoCloseable {

    private final ExecutorService executorService =
            newCachedThreadPool(r -> new Thread(r, "client-handler-thread"));
    private volatile boolean running = true;

    public void start(RedisServerConfiguration serverConfiguration) throws IOException {
        ServerSocket serverSocket = new ServerSocket(serverConfiguration.getPort());
        serverSocket.setReuseAddress(true);

        // if there is master host and master port, then it's a slave server and we need to replicate from master
        if (serverConfiguration.getMasterHost() != null && serverConfiguration.getMasterPort() != null) {
            ReplicationManager.replicateFrom(
                    serverConfiguration.getMasterHost(),
                    serverConfiguration.getMasterPort(),
                    serverConfiguration.getPort()
            );
        }

        // if rdb file specified, read from it
        if (serverConfiguration.getRdbFilenamePath() != null) {
            RdbPersistenceManager
                    .init(serverConfiguration.getRdbFilenamePath())
                    .readFromDbFile();
        }

        // if aof enabled, init
        if (serverConfiguration.isAppendOnlyEnabled()) {
            AofPersistenceManager
                    .init(
                            Path.of(serverConfiguration.getDir()),
                            true,
                            serverConfiguration.getAppendDirName(),
                            serverConfiguration.getAppendFilename(),
                            serverConfiguration.getAppendFSync()
                    )
                    .replayFromAofFile();
        }

        Logger.info("Redis server is listening on port {}", serverConfiguration.getPort());
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                Logger.info("Accepted connection from {}", clientSocket.getRemoteSocketAddress());
                executorService.submit(new RedisClientHandler(clientSocket));
            } catch (Exception e) {
                Logger.error("Error accepting client connection: {}", e.getMessage(), e);
            }
        }

        serverSocket.close();
    }

    @Override
    public void close() {
        running = false;
        executorService.shutdown();
    }
}
