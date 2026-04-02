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

    public void setContent(Object content) {
        this.content = content;
    }

    public void setType(RedisMessageType type) {
        this.type = type;
    }

    public Object getContent() {
        return content;
    }

    public RedisMessageType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "RedisMessage{" +
                "content=" + content +
                ", type=" + type +
                '}';
    }
}
