package serdes;

import java.util.List;

public class RedisSerializer {

    private RedisSerializer() {}

    public static byte[] serialize(final RedisMessage redisMessage) {
        StringBuilder sb = new StringBuilder();
        switch (redisMessage.getType()) {
            case SIMPLE_STRING:
                sb.append("+").append(redisMessage.getContent()).append("\r\n");
                break;
            case ERROR:
                sb.append("-").append(redisMessage.getContent()).append("\r\n");
                break;
            case INTEGER:
                sb.append(":").append(redisMessage.getContent()).append("\r\n");
                break;
            case BULK_STRING:
                if (redisMessage.getContent() == null) {
                    sb.append("$-1\r\n");
                } else {
                    String str = redisMessage.getContent().toString();
                    sb.append("$").append(str.length()).append("\r\n").append(str).append("\r\n");
                }
                break;
            case ARRAY:
                if (redisMessage.getContent() == null) {
                    sb.append("*-1\r\n");
                } else {
                    List<RedisMessage> messages = (List<RedisMessage>) redisMessage.getContent();
                    sb.append("*").append(messages.size()).append("\r\n");
                    for (RedisMessage msg : messages) {
                        sb.append(new String(serialize(msg)));
                    }
                }
                break;
        }
        return sb.toString().getBytes();
    }

    public static byte[] nullBulkString() {
        return "$-1\r\n".getBytes();
    }

    public static byte[] okString() {
        return "+OK\r\n".getBytes();
    }

    public static byte[] integer(int value) {
        RedisMessage redisMessage = new RedisMessage();
        redisMessage.setType(RedisMessage.RedisMessageType.INTEGER);
        redisMessage.setContent(value);
        return serialize(redisMessage);
    }

    public static byte[] list(List<RedisMessage> messages) {
        RedisMessage redisMessage = new RedisMessage();
        redisMessage.setType(RedisMessage.RedisMessageType.ARRAY);
        redisMessage.setContent(messages);
        return serialize(redisMessage);
    }

    public static byte[] listRaw(List<byte[]> messagesRaw) {
        StringBuilder sb = new StringBuilder()
                .append("*").append(messagesRaw.size()).append("\r\n");

        for (byte[] msg : messagesRaw) {
            sb.append(new String(msg));
        }

        return sb.toString().getBytes();
    }

    public static byte[] nullArray() {
        return "*-1\r\n".getBytes();
    }

    public static byte[] bulkString(String key) {
        RedisMessage redisMessage = new RedisMessage();
        redisMessage.setType(RedisMessage.RedisMessageType.BULK_STRING);
        redisMessage.setContent(key);
        return serialize(redisMessage);
    }

    public static byte[] simpleString(String value) {
        RedisMessage message = new RedisMessage();
        message.setType(RedisMessage.RedisMessageType.SIMPLE_STRING);
        message.setContent(value);
        return serialize(message);
    }

    public static byte[] error(String error) {
        RedisMessage message = new RedisMessage();
        message.setType(RedisMessage.RedisMessageType.ERROR);
        message.setContent(error);
        return serialize(message);
    }
}
