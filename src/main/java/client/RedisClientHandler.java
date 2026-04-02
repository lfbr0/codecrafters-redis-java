package client;

import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;

import java.io.IOException;
import java.net.Socket;

public class RedisClientHandler implements Runnable {

    private final Socket socket;

    public RedisClientHandler(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            while (socket.isConnected()) {
                RedisMessage message = RedisDeserializer.deserialize(socket.getInputStream());
                Logger.info("Received message: " + message);
                // Do nothing for now, just respond to PING with PONG
                socket.getOutputStream().write("+PONG\r\n".getBytes());
            }
            socket.getOutputStream().write("+PONG\r\n".getBytes());
        } catch (IOException e) {
            Logger.error("Failed to interpret command: {}", e.getMessage(), e);
        }
    }
}
