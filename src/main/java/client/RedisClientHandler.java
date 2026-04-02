package client;

import commands.Command;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

@Slf4j
@RequiredArgsConstructor
public class RedisClientHandler implements Runnable {

    private final Socket socket;

    @Override
    public void run() {
        try {
            socket.getOutputStream().write("+PONG\r\n".getBytes());
        } catch (IOException e) {
            log.error("Failed to interpret command: {}", e.getMessage(), e);
        }
    }
}
