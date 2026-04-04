package serdes;

import logger.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RedisDeserializer {

    private RedisDeserializer() {}

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
                return null;
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
                        redisMessage.setContent(null);
                    } else {
                        byte[] bulkData = new byte[length];
                        int totalRead = 0;

                        while (totalRead < length) {
                            int bytesRead = inputStream.read(bulkData, totalRead, length - totalRead);
                            if (bytesRead == -1) {
                                throw new IOException("Unexpected end of stream while reading bulk string");
                            }
                            totalRead += bytesRead;
                        }

                        for (byte b : bulkData) {
                            contentByteList.add(b);
                        }

                        int r = inputStream.read();
                        int n = inputStream.read();

                        if (r != '\r' || n != '\n') {
                            throw new IOException("Invalid bulk string termination");
                        }

                        contentByteList.add((byte) r);
                        contentByteList.add((byte) n);

                        redisMessage.setContent(new String(bulkData, StandardCharsets.UTF_8));
                    }
                    break;

                case '*':
                    redisMessage.setType(RedisMessage.RedisMessageType.ARRAY);
                    int arrayLength = Integer.parseInt(readLine(inputStream, contentByteList));
                    List<RedisMessage> arrayElements = new ArrayList<>();
                    for (int i = 0; i < arrayLength; i++) {
                        RedisMessage element = deserialize(inputStream, contentByteList);
                        if (element == null) {
                            throw new IOException("Unexpected end of stream while reading array element");
                        }
                        arrayElements.add(element);
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

    private static byte[] byteListToArray(List<Byte> byteList) {
        byte[] array = new byte[byteList.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = byteList.get(i);
        }
        return array;
    }

    private static String readLine(InputStream inputStream, List<Byte> contentByteList) throws IOException {
        StringBuilder line = new StringBuilder();
        int prevChar = 0;
        int currentChar;

        while ((currentChar = inputStream.read()) != -1) {
            contentByteList.add((byte) currentChar);
            if (prevChar == '\r' && currentChar == '\n') {
                line.setLength(line.length() - 1);
                return line.toString();
            }
            line.append((char) currentChar);
            prevChar = currentChar;
        }

        throw new IOException("Unexpected end of stream while reading line");
    }

    public static RedisMessage deserialize(byte[] bytes) throws IOException {
        return deserialize(new ByteArrayInputStream(bytes));
    }
}