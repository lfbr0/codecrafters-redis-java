import logger.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.Executors.newCachedThreadPool;

public class RedisServer implements AutoCloseable {

    private final ExecutorService executorService =
            newCachedThreadPool(r -> new Thread(r, "client-handler-thread"));
    private volatile boolean running = true;

    public void start(RedisServerConfiguration redisServerConfiguration) throws IOException {
        ServerSocket serverSocket = new ServerSocket(redisServerConfiguration.getPort());
        serverSocket.setReuseAddress(true);

        Logger.info("Redis server is listening on port {}", redisServerConfiguration.getPort());
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
