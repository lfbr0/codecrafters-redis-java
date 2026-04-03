package replication;

import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;
import serdes.RedisSerializer;

import java.net.Socket;
import java.util.List;

public class SlaveReplicationThread extends Thread {

    private final String masterHost;
    private final int masterPort;
    private final int myPort;
    private String masterReplicationId;

    public SlaveReplicationThread(String masterHost, int masterPort, int myPort) {
        super("RedisSlaveReplicationThread");
        setDaemon(true); // this is an auxiliary thread, we don't want to block jvm kill
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.myPort = myPort;
    }

    @Override
    public void run() {
        try {
            Logger.info("\t\tStarting replication from master at host=" + masterHost + ", port=" + masterPort);
            Socket socket = new Socket(masterHost, masterPort);

            // phase 1 - send PING as RESP array & expect PONG back
            socket.getOutputStream().write(RedisSerializer.listStrings(List.of("PING")));
            // read message back
            RedisMessage masterResp = RedisDeserializer.deserialize(socket.getInputStream().readNBytes(7));
            Logger.info("\t\tPhase 1 - Received message from master: " + masterResp);
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().equalsIgnoreCase("PONG");

            // phase 2 - evoke REPLCONF command on master & send him info
            List<String> replConfList = List.of("REPLCONF", "listening-port", Integer.toString(myPort));
            socket.getOutputStream().write(RedisSerializer.listStrings(replConfList));
            masterResp = RedisDeserializer.deserialize(socket.getInputStream().readNBytes(5));
            Logger.info("\t\tPhase 2 - Received message from master: " + masterResp);
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().equalsIgnoreCase("OK");
            // send out capa psync2
            replConfList = List.of("REPLCONF", "capa", "psync2");
            socket.getOutputStream().write(RedisSerializer.listStrings(replConfList));
            masterResp = RedisDeserializer.deserialize(socket.getInputStream().readNBytes(5));
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().equalsIgnoreCase("OK");

            // phase 3 - send PSYNC and get replication id
            List<String> psyncList = List.of("PSYNC", "?", "-1");
            socket.getOutputStream().write(RedisSerializer.listStrings(psyncList));
            masterResp = RedisDeserializer.deserialize(socket.getInputStream().readNBytes(52));
            Logger.info("\t\tPhase 3 - Received message from master " + masterResp);
            assert masterResp != null && masterResp.getType() == RedisMessage.RedisMessageType.SIMPLE_STRING;
            assert masterResp.getContent().toString().startsWith("FULLRESYNC");
            // save replication id from master
            this.masterReplicationId = masterResp.getContent().toString().split(" ")[1];
            Logger.info("\t\tReceived master replication id from master=[" + masterReplicationId + "]");

        } catch (Exception ex) {
            Logger.error("\t\tFailed to replicate: " + ex.getMessage(), ex);
        }
    }

    /**
     * Returns master replication id gathered from replication phases
     * @return master replication id
     */
    public String getMasterReplicationId() {
        return masterReplicationId;
    }
}
