package serdes;

import logger.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RedisDeserializer {

    private static final String INTEGER_REGEX_PATTERN = "\\d+";

    private RedisDeserializer() {}

    /**
     * Deserializes a Redis message from the given InputStream.
     * @param inputStream socket input stream to read the Redis message from
     * @return the deserialized RedisMessage object
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static RedisMessage deserialize(InputStream inputStream) throws IOException {
        try {
            RedisMessage redisMessage = new RedisMessage();

            int firstByte = inputStream.read();
            if (firstByte == -1) {
                Logger.info("End of stream reached while trying to read Redis message.");
                return null; // End of stream, return null to indicate no more messages
            }

            switch (firstByte) {
                case '+':
                    redisMessage.setType(RedisMessage.RedisMessageType.SIMPLE_STRING);
                    redisMessage.setContent(readLine(inputStream));
                    break;
                case '-':
                    redisMessage.setType(RedisMessage.RedisMessageType.ERROR);
                    redisMessage.setContent(readLine(inputStream));
                    break;
                case ':':
                    redisMessage.setType(RedisMessage.RedisMessageType.INTEGER);
                    redisMessage.setContent(readLine(inputStream));
                    break;
                case '$':
                    redisMessage.setType(RedisMessage.RedisMessageType.BULK_STRING);
                    int length = Integer.parseInt(readLine(inputStream));
                    if (length == -1) {
                        redisMessage.setContent(null); // Null bulk string
                    } else {
                        byte[] bulkData = new byte[length];
                        inputStream.read(bulkData);
                        inputStream.read(); // Read the trailing \r
                        inputStream.read(); // Read the trailing \n
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
                    int arrayLength = Integer.parseInt(readLine(inputStream));
                    List<RedisMessage> arrayElements = new ArrayList<>();
                    for (int i = 0; i < arrayLength; i++) {
                        arrayElements.add(deserialize(inputStream)); // Recursively deserialize each element
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
     * Reads a line from the InputStream, terminated by \r\n.
     * @param inputStream the InputStream to read from
     * @return the line read from the InputStream, without the trailing \r\n
     */
    private static String readLine(InputStream inputStream) throws IOException {
        StringBuilder line = new StringBuilder();
        int prevChar = 0;
        int currentChar;

        while ((currentChar = inputStream.read()) != -1) {
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
