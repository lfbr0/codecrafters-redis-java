package serdes;

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
                    sb.append("*").append(((Iterable<?>) redisMessage.getContent()).spliterator().getExactSizeIfKnown()).append("\r\n");
                    for (Object item : (Iterable<?>) redisMessage.getContent()) {
                        sb.append(serialize((RedisMessage) item));
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
}
