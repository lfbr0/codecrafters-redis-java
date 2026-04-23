import java.nio.file.Path;

public class RedisServerConfiguration {

    public static final int DEFAULT_PORT = 6379;

    private final int port;
    private final String masterHost;
    private final Integer masterPort;
    private final String dir;
    private final String dbFilename;

    public RedisServerConfiguration(int port) {
        this.port = port;
        this.masterHost = null;
        this.masterPort = null;
        this.dir = null;
        this.dbFilename = null;
    }

    public RedisServerConfiguration(int port, String masterHost, Integer masterPort) {
        this.port = port;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.dir = null;
        this.dbFilename = null;
    }

    public RedisServerConfiguration(int port, String masterHost, Integer masterPort, String dir, String dbFilename) {
        this.port = port;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.dir = dir;
        this.dbFilename = dbFilename;
    }

    /**
     * Returns Redis Configuration for Server Master
     * @return default config
     */
    public static RedisServerConfiguration defaultConfiguration() {
        return new RedisServerConfiguration(DEFAULT_PORT);
    }

    /**
     * Returns Redis Configuration
     * @param args server arguments
     * @return default config if args null or empty, otherwise parsed config
     */
    public static RedisServerConfiguration args(String[] args) {
        if (args == null || args.length == 0) {
            return defaultConfiguration();
        }

        // default values
        int port = DEFAULT_PORT;
        String masterHost = null;
        Integer masterPort = null;
        String dir = null;
        String dbFilename = null;

        for (int i = 0; i < args.length; i++) {
            // check if arg is --port
            if ("--port".equalsIgnoreCase(args[i])) {
                port = Integer.parseInt(args[++i]);
                continue;
            }
            // check if replica of
            if ("--replicaof".equalsIgnoreCase(args[i])) {
                String[] replicaOfArgs = args[++i].split("\\s+");
                masterHost = replicaOfArgs[0];
                masterPort = Integer.parseInt(replicaOfArgs[1]);
            }
            // check if dir
            if ("--dir".equalsIgnoreCase(args[i])) {
                dir = args[++i];
            }
            // check if dbfilename is set
            if ("--dbfilename".equalsIgnoreCase(args[i])) {
                dbFilename = args[++i];
            }
        }

        return new RedisServerConfiguration(port, masterHost, masterPort, dir, dbFilename);
    }

    public int getPort() {
        return port;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public Integer getMasterPort() {
        return masterPort;
    }

    public Path getDbFilenamePath() {
        if (dir == null || dbFilename == null) {
            return null;
        }
        return Path.of(dir, dbFilename);
    }
}
