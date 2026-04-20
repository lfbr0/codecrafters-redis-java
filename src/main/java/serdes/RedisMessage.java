package serdes;

public class RedisMessage {

    /**
     * Enum representing the different types of Redis messages.
     */
    public enum RedisMessageType {
        ERROR, INTEGER, BULK_STRING, ARRAY, SIMPLE_STRING
    }

    private Object content;
    private RedisMessageType type;
    private byte[] contentBytes;

    public RedisMessage setContent(Object content) {
        this.content = content;
        return this;
    }

    public RedisMessage setType(RedisMessageType type) {
        this.type = type;
        return this;
    }

    public RedisMessage setContentBytes(byte[] contentBytes) {
        this.contentBytes = contentBytes;
        return this;
    }

    public Object getContent() {
        return content;
    }

    public RedisMessageType getType() {
        return type;
    }

    public byte[] getContentBytes() {
        return contentBytes;
    }

    @Override
    public String toString() {
        return "RedisMessage{" +
                "content=" + content +
                ", type=" + type +
                '}';
    }
}
