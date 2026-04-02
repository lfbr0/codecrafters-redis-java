package client;

import commands.Command;
import logger.Logger;

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
            socket.getOutputStream().write("+PONG\r\n".getBytes());
        } catch (IOException e) {
            Logger.error("Failed to interpret command: {}", e.getMessage(), e);
        }
    }
}
