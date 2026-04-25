package data;

import logger.Logger;
import serdes.RedisMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

public class PersistenceManager {

    private static PersistenceManager INSTANCE;
    private final Path dbFilenamePath;

    // for state mgmt
    private final List<String> persistedKeys = new LinkedList<>();


    public PersistenceManager(Path dbFilenamePath) {
        this.dbFilenamePath = dbFilenamePath;
    }

    public static synchronized PersistenceManager init(Path dbFilenamePath) {
        if (INSTANCE == null) {
            INSTANCE = new PersistenceManager(dbFilenamePath);
        }
        return INSTANCE;
    }

    public synchronized static Optional<PersistenceManager> getInstance() {
        return Optional.ofNullable(INSTANCE);
    }


    public String getDbFileDir() {
        return dbFilenamePath.toFile().getParent();
    }

    public String getDbFilename() {
        return dbFilenamePath.toFile().getName();
    }

    public List<String> getPersistedKeys() {
        return new ArrayList<>(persistedKeys);
    }

    /**
     * Reads RDB file as per structure
     * Link: https://rdb.fnordig.de/file_format.html
     */
    public void readFromDbFile() {
        if (dbFilenamePath == null) {
            Logger.error("Persistence Manager - will not continue, Database path is null");
            return;
        }

        File dbFile = dbFilenamePath.toFile();
        if (!dbFile.exists()) {
            Logger.error("Persistence Manager - will not continue, Database path does not exist");
            return;
        }

        try (FileInputStream fis = new FileInputStream(dbFile)) {
            // start by reading header and asserting it is what is expected
            byte[] header = fis.readNBytes(5);
            if (!new String(header).startsWith("REDIS")) {
                Logger.error("Persistence Manager - will not continue, Database path is not REDIS");
                return;
            }

            byte[] rdbVersion = fis.readNBytes(4);
            Logger.info("Persistence Manager - reading file of RDB version: " + parseInt(new String(rdbVersion)));

            int b;
            while ((b = fis.read()) != -1) {

                // AUX
                if (b == 0xFA) {
                    readString(fis);
                    readString(fis);
                }

                // DB selector
                else if (b == 0xFE) {
                    int dbSelectorIdx = readLengthEncodedInt(fis);
                    interpretDatabaseSelector(dbSelectorIdx, new LinkedList<>(), fis);
                }

                // EOF
                else if (b == 0xFF) {
                    break;
                }
            }

        } catch (Exception e) {
            Logger.error("Persistence Manager - will not continue, Error reading from file " + e);
        }
    }

    private void interpretDatabaseSelector(int dbSelectorIdx, List<Runnable> inserts, FileInputStream fis) throws IOException {
        Logger.info("Reading DB selector " + dbSelectorIdx);

        if (fis.read() != 0xFB) {
            throw new IOException("DB selector read error, expected RESIZEDB FIELD!");
        }

        // size of the corresponding hash table (length-encoded-int)
        int hashTableSize = readLengthEncodedInt(fis);

        // size of the corresponding expire hash table (length-encoded-int)
        int expireHashTableSize = readLengthEncodedInt(fis);

        Logger.info("Expect hashTableSize=" + hashTableSize + " and expireHashTableSize=" + expireHashTableSize);
        int readHashTableSize = 0, readExpireHashTableSize = 0;

        int rb = fis.read();
        while (rb != 0xFE && rb != 0xFF && rb != -1) {
            switch (rb) {
                // "expiry time in seconds"
                case 0xFD: {
                    interpretKeyValue(fis, inserts, TimeUnit.SECONDS);
                    if (readExpireHashTableSize++ > expireHashTableSize) {
                        throw new IOException("Read expire hash table size surpasses announced size in RDB file!");
                    }
                    break;
                }
                // "expiry time in ms"
                case 0xFC: {
                    interpretKeyValue(fis, inserts, TimeUnit.MILLISECONDS);
                    if (readExpireHashTableSize++ > expireHashTableSize) {
                        throw new IOException("Read expire hash table size surpasses announced size in RDB file!");
                    }
                    break;
                }
                // key-value pair without expiry
                default: {
                    interpretKeyValueWithKnownType(fis, inserts, null, rb);
                    if (readHashTableSize++ > hashTableSize) {
                        throw new IOException("Read hash table size surpasses announced size in RDB file!");
                    }
                    break;
                }
            }

            rb = fis.read();
        }
        // end-while
    }

    private void interpretKeyValue(FileInputStream fis, List<Runnable> inserts, TimeUnit timeUnit) {
        Duration expiry = null;

        try {
            // FC $unsigned long
            if (timeUnit == TimeUnit.MILLISECONDS) {
                expiry = Duration.ofMillis(readUInt64(fis));
            }
            // FD $unsigned-int
            else if (timeUnit == TimeUnit.SECONDS) {
                expiry = Duration.ofSeconds(readUInt32(fis));
            }

            int valueType = readByte(fis);
            interpretKeyValueWithKnownType(fis, inserts, expiry, valueType);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void interpretKeyValueWithKnownType(FileInputStream fis,
                                                List<Runnable> inserts,
                                                Duration expiry, // #nullable
                                                int valueType) throws IOException {
        String key;
        String value;

        if (valueType != 0) {
            throw new IOException("Only STRING encoding supported for now!");
        }

        key = readString(fis);
        value = readString(fis);
        RedisMessage valueAsRedisMessage = new RedisMessage()
                .setType(RedisMessage.RedisMessageType.BULK_STRING)
                .setContent(value);

        persistedKeys.add(key);
        Logger.info("Persistence Manager - got KV -> key=" + key + ", value=" + value + ", expiry=" + expiry);
        inserts.add(() -> MemoryManager.set(key, valueAsRedisMessage, expiry));
    }

    private String readString(FileInputStream fis) throws IOException {
        int firstByte = readByte(fis);
        int type = (firstByte >>> 6) & 0b11;

        if (type == 0b11) {
            int enc = firstByte & 0x3F;

            switch (enc) {
                case 0:
                    return String.valueOf((byte) readByte(fis));
                case 1: {
                    int lo = readByte(fis);
                    int hi = readByte(fis);
                    return String.valueOf((short) ((hi << 8) | lo));
                }
                case 2: {
                    int b1 = readByte(fis);
                    int b2 = readByte(fis);
                    int b3 = readByte(fis);
                    int b4 = readByte(fis);
                    int val = (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
                    return String.valueOf(val);
                }
                default:
                    throw new IOException("Unsupported special string encoding");
            }
        }

        int length = readLengthFromFirstByte(fis, firstByte, type);
        byte[] data = fis.readNBytes(length);

        if (data.length < length) {
            throw new IOException("Unexpected EOF");
        }

        return new String(data, StandardCharsets.UTF_8);
    }

    private int readLengthFromFirstByte(FileInputStream fis, int firstByte, int type) throws IOException {
        switch (type) {
            case 0b00:
                return firstByte & 0b00111111;
            case 0b01:
                return ((firstByte & 0b00111111) << 8) | readByte(fis);
            case 0b10:
                return readUInt32(fis);
            default:
                throw new IOException("Invalid encoding");
        }
    }

    private int readLengthEncodedInt(FileInputStream fis) throws IOException {
        int firstByte = fis.read();
        if (firstByte == -1) {
            throw new IOException("Unexpected EOF");
        }

        int type = (firstByte >>> 6) & 0b11;
        int parsedLength;

        switch (type) {
            // The next 6 bits represent the length
            case 0b00: {
                parsedLength = firstByte & 0b00111111;
                break;
            }
            // Read one additional byte. The combined 14 bits represent the length
            case 0b01: {
                int secondByte = fis.read();
                if (secondByte == -1) {
                    throw new IOException("Unexpected EOF");
                }
                parsedLength = ((firstByte & 0b00111111) << 8) | secondByte;
                break;
            }
            // Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            case 0b10: {
                byte[] buffer = fis.readNBytes(4);
                if (buffer.length < 4) {
                    throw new IOException("Unexpected EOF");
                }

                parsedLength =
                        ((buffer[0] & 0xFF) << 24) |
                                ((buffer[1] & 0xFF) << 16) |
                                ((buffer[2] & 0xFF) << 8)  |
                                (buffer[3] & 0xFF);
                break;
            }
            default: throw new IOException("No support for special formats!");
        }

        return parsedLength;
    }

    private int readByte(FileInputStream fis) throws IOException {
        int b = fis.read();
        if (b == -1) throw new IOException("Unexpected EOF");
        return b & 0xFF;
    }

    private int readUInt32(FileInputStream fis) throws IOException {
        return (readByte(fis) << 24)
                | (readByte(fis) << 16)
                | (readByte(fis) << 8)
                | readByte(fis);
    }

    private long readUInt64(FileInputStream fis) throws IOException {
        return ((long) readByte(fis) << 56)
                | ((long) readByte(fis) << 48)
                | ((long) readByte(fis) << 40)
                | ((long) readByte(fis) << 32)
                | ((long) readByte(fis) << 24)
                | ((long) readByte(fis) << 16)
                | ((long) readByte(fis) << 8)
                | readByte(fis);
    }
}