public class RedisServerConfiguration {

    public static final int DEFAULT_PORT = 6379;

    private final int port;

    public RedisServerConfiguration(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public static RedisServerConfiguration defaultConfiguration() {
        return new RedisServerConfiguration(DEFAULT_PORT);
    }
}
