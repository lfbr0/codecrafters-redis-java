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

    public void setContent(Object content) {
        this.content = content;
    }

    public void setType(RedisMessageType type) {
        this.type = type;
    }

    public void setContentBytes(byte[] contentBytes) {
        this.contentBytes = contentBytes;
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
