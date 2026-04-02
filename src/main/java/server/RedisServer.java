package server;

import client.RedisClientHandler;
import logger.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer implements AutoCloseable {

    private final ExecutorService executorService = Executors.newCachedThreadPool();
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
