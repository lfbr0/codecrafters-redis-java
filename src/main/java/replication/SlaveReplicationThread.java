package replication;

import handler.RedisClientHandler;
import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class SlaveReplicationThread extends Thread {

    private final String masterHost;
    private final int masterPort;
    private final int myPort;
    private final BlockingQueue<String> masterReplIdQueue;
    private Runnable slaveReplicationHandler;

    public SlaveReplicationThread(String masterHost, int masterPort, int myPort, BlockingQueue<String> masterReplIdQueue) {
        super("RedisSlaveReplicationThread");
        setDaemon(true);
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.myPort = myPort;
        this.masterReplIdQueue = masterReplIdQueue;
    }

    public Runnable getSlaveReplicationHandler() {
        return slaveReplicationHandler;
    }

    @Override
    public void run() {
        try {
            Logger.info("\t\tStarting replication from master at host=" + masterHost + ", port=" + masterPort);
            Socket socket = new Socket(masterHost, masterPort);

            socket.getOutputStream().write(RedisSerializer.listStrings(List.of("PING")));
            RedisMessage masterResp = RedisDeserializer.deserialize(socket.getInputStream());
            Logger.info("\t\tPhase 1 - Received message from master: " + masterResp);
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().equalsIgnoreCase("PONG");

            List<String> replConfList = List.of("REPLCONF", "listening-port", Integer.toString(myPort));
            socket.getOutputStream().write(RedisSerializer.listStrings(replConfList));
            masterResp = RedisDeserializer.deserialize(socket.getInputStream());
            Logger.info("\t\tPhase 2 - Received message from master: " + masterResp);
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().equalsIgnoreCase("OK");

            replConfList = List.of("REPLCONF", "capa", "psync2");
            socket.getOutputStream().write(RedisSerializer.listStrings(replConfList));
            masterResp = RedisDeserializer.deserialize(socket.getInputStream());
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().equalsIgnoreCase("OK");

            List<String> psyncList = List.of("PSYNC", "?", "-1");
            socket.getOutputStream().write(RedisSerializer.listStrings(psyncList));
            masterResp = RedisDeserializer.deserialize(socket.getInputStream());
            Logger.info("\t\tPhase 3 - Received message from master " + masterResp);
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().startsWith("FULLRESYNC");

            String masterReplicationId = masterResp.getContent().toString().split(" ")[1];
            Logger.info("\t\tReceived master replication id from master=[" + masterReplicationId + "]");
            masterReplIdQueue.offer(masterReplicationId);

            consumeRdbFile(socket.getInputStream());
            Logger.info("\t\tConsumed RDB payload from master");

            this.slaveReplicationHandler = new RedisClientHandler(socket, false);
        } catch (Exception ex) {
            Logger.error("\t\tFailed to replicate: " + ex.getMessage(), ex);
        }
    }

    private static void consumeRdbFile(InputStream inputStream) throws IOException {
        int firstByte = inputStream.read();
        if (firstByte == -1) {
            throw new IOException("Unexpected end of stream before RDB payload");
        }
        if (firstByte != '$') {
            throw new IOException("Expected RDB bulk length prefix '$', got: " + (char) firstByte);
        }

        String lengthLine = readLine(inputStream);
        int length = Integer.parseInt(lengthLine);

        if (length < 0) {
            throw new IOException("Invalid RDB length: " + length);
        }

        byte[] rdbBytes = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            int bytesRead = inputStream.read(rdbBytes, totalRead, length - totalRead);
            if (bytesRead == -1) {
                throw new IOException("Unexpected end of stream while reading RDB file");
            }
            totalRead += bytesRead;
        }

        // In this Codecrafters stage, the RDB payload is raw length-prefixed bytes.
        // Do not attempt to consume trailing CRLF here.
    }

    private static String readLine(InputStream inputStream) throws IOException {
        StringBuilder line = new StringBuilder();
        int prevChar = 0;
        int currentChar;

        while ((currentChar = inputStream.read()) != -1) {
            if (prevChar == '\r' && currentChar == '\n') {
                line.setLength(line.length() - 1);
                return line.toString();
            }
            line.append((char) currentChar);
            prevChar = currentChar;
        }

        throw new IOException("Unexpected end of stream while reading line");
    }
}