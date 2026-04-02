package server;

import client.RedisClientHandler;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisServer implements AutoCloseable {

    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean running = true;

    public void start(RedisServerConfiguration redisServerConfiguration) throws IOException {
        ServerSocket serverSocket = new ServerSocket(redisServerConfiguration.getPort());
        serverSocket.setReuseAddress(true);

        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(new RedisClientHandler(clientSocket));
            } catch (Exception e) {
                log.error("Error accepting client connection: {}", e.getMessage(), e);
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
