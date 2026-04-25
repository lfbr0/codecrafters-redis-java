package handler;

import logger.Logger;
import serdes.RedisDeserializer;
import serdes.RedisMessage;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class AofRedisClientHandler extends AbstractRedisClientHandler {

    private final File aofFile;

    public AofRedisClientHandler(File aofFile) {
        this.aofFile = aofFile;
    }

    @Override
    public void run() {
        Logger.info("AOF - Processing AOF file=" + aofFile);

        try (FileInputStream fis = new FileInputStream(aofFile)) {
            while (true) {
                RedisMessage message = RedisDeserializer.deserialize(fis);
                if (message == null) {
                    break; // EOF
                }

                Logger.info("AOF - replaying command " + message);
                handleMessage(message, new AofDummyOutputStream());
            }
        } catch (Exception ex) {
            Logger.error("AOF - Failed to process AOF file", ex);
        }
    }

    private static class AofDummyOutputStream extends OutputStream {
        @Override
        public void write(int i) {
        }
    }
}
