public class RedisServerConfiguration {

    public static final int DEFAULT_PORT = 6379;

    private final int port;

    public RedisServerConfiguration(int port) {
        this.port = port;
    }

    public static RedisServerConfiguration args(String[] args) {
        if (args == null || args.length == 0) {
            return defaultConfiguration();
        }

        // default values
        int port = DEFAULT_PORT;

        for (int i = 0; i < args.length; i++) {
            // check if arg is --port
            if ("--port".equalsIgnoreCase(args[i])) {
                port = Integer.parseInt(args[++i]);
                continue;
            }
        }

        return new RedisServerConfiguration(port);
    }

    public int getPort() {
        return port;
    }

    public static RedisServerConfiguration defaultConfiguration() {
        return new RedisServerConfiguration(DEFAULT_PORT);
    }
}
