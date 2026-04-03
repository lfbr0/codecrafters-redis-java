package serdes;

import logger.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RedisDeserializer {

    private static final String INTEGER_REGEX_PATTERN = "\\d+";

    private RedisDeserializer() {}

    /**
     * Deserializes a Redis message from the given InputStream.
     * @param inputStream socket input stream to read the Redis message from
     * @return the deserialized RedisMessage object
     */
    public static RedisMessage deserialize(InputStream inputStream) throws IOException {
        List<Byte> contentByteList = new LinkedList<>();
        RedisMessage redisMessage = deserialize(inputStream, contentByteList);
        if (redisMessage != null) {
            redisMessage.setContentBytes(byteListToArray(contentByteList));
        }
        return redisMessage;
    }

    private static RedisMessage deserialize(InputStream inputStream, List<Byte> contentByteList) throws IOException {
        try {
            RedisMessage redisMessage = new RedisMessage();

            int firstByte = inputStream.read();
            if (firstByte == -1) {
                Logger.info("End of stream reached while trying to read Redis message.");
                return null; // End of stream, return null to indicate no more messages
            }
            contentByteList.add((byte) firstByte);

            switch (firstByte) {
                case '+':
                    redisMessage.setType(RedisMessage.RedisMessageType.SIMPLE_STRING);
                    redisMessage.setContent(readLine(inputStream, contentByteList));
                    break;
                case '-':
                    redisMessage.setType(RedisMessage.RedisMessageType.ERROR);
                    redisMessage.setContent(readLine(inputStream, contentByteList));
                    break;
                case ':':
                    redisMessage.setType(RedisMessage.RedisMessageType.INTEGER);
                    redisMessage.setContent(readLine(inputStream, contentByteList));
                    break;
                case '$':
                    redisMessage.setType(RedisMessage.RedisMessageType.BULK_STRING);
                    int length = Integer.parseInt(readLine(inputStream, contentByteList));
                    if (length == -1) {
                        redisMessage.setContent(null); // Null bulk string
                    } else {
                        byte[] bulkData = new byte[length];
                        inputStream.read(bulkData);
                        for (byte b : bulkData) {
                            contentByteList.add(b);
                        }
                        int r = inputStream.read();// Read the trailing \r
                        contentByteList.add((byte) r);
                        int n = inputStream.read(); // Read the trailing \n
                        contentByteList.add((byte) n);
                        String bulkString = new String(bulkData);
                        // fix - if just numbers, treat as integer
                        if (bulkString.matches(INTEGER_REGEX_PATTERN)) {
                            // treat as integer
                            redisMessage.setType(RedisMessage.RedisMessageType.INTEGER);
                            redisMessage.setContent(Integer.parseInt(bulkString));
                        } else {
                            // proceed as bulk string
                            redisMessage.setContent(bulkString);
                        }
                    }
                    break;
                case '*':
                    redisMessage.setType(RedisMessage.RedisMessageType.ARRAY);
                    int arrayLength = Integer.parseInt(readLine(inputStream, contentByteList));
                    List<RedisMessage> arrayElements = new ArrayList<>();
                    for (int i = 0; i < arrayLength; i++) {
                        arrayElements.add(deserialize(inputStream, contentByteList)); // Recursively deserialize each element
                    }
                    redisMessage.setContent(arrayElements);
                    break;
                default:
                    throw new IOException("Invalid Redis message type: " + (char) firstByte);
            }

            return redisMessage;
        } catch (IOException e) {
            Logger.error("Failed to deserialize Redis message: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Convert byte list to byte array
     * @param byteList byte list to convert
     * @return array of bytes
     */
    private static byte[] byteListToArray(List<Byte> byteList) {
        byte[] array = new byte[byteList.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = byteList.get(i);
        }
        return array;
    }

    /**
     * Reads a line from the InputStream, terminated by \r\n.
     * @param inputStream the InputStream to read from
     * @return the line read from the InputStream, without the trailing \r\n
     */
    private static String readLine(InputStream inputStream, List<Byte> contentByteList) throws IOException {
        StringBuilder line = new StringBuilder();
        int prevChar = 0;
        int currentChar;

        while ((currentChar = inputStream.read()) != -1) {
            contentByteList.add((byte) currentChar);
            if (prevChar == '\r' && currentChar == '\n') {
                line.setLength(line.length() - 1); // Remove the trailing \r
                break;
            }
            line.append((char) currentChar);
            prevChar = currentChar;
        }

        return line.toString();
    }

    public static RedisMessage deserialize(byte[] bytes) throws IOException {
        return deserialize(new ByteArrayInputStream(bytes));
    }
}
