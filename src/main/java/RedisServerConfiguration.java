import data.AofPersistenceManager;

import java.nio.file.Path;

public class RedisServerConfiguration {

    private int port = 6379;
    private String masterHost = null;
    private Integer masterPort = null;
    // RDB
    private String dir = null;
    private String dbFilename = null;
    // AOF
    boolean appendOnlyEnabled = false;
    String appendDirName = null;
    String appendFilename = null;
    private AofPersistenceManager.FsyncFrequency appendFSync;

    /**
     * Returns Redis Configuration for Server Master
     * @return default config
     */
    public static RedisServerConfiguration defaultConfiguration() {
        return new RedisServerConfiguration();
    }

    /**
     * Returns Redis Configuration
     * @param args server arguments
     * @return default config if args null or empty, otherwise parsed config
     */
    public static RedisServerConfiguration args(String[] args) {
        RedisServerConfiguration conf = defaultConfiguration();

        if (args == null || args.length == 0) {
            return conf;
        }

        for (int i = 0; i < args.length; i++) {
            // check if arg is --port
            if ("--port".equalsIgnoreCase(args[i])) {
                conf.setPort(Integer.parseInt(args[++i]));
            }
            // check if replica of
            if ("--replicaof".equalsIgnoreCase(args[i])) {
                String[] replicaOfArgs = args[++i].split("\\s+");
                conf.setMasterHost(replicaOfArgs[0])
                    .setMasterPort(Integer.parseInt(replicaOfArgs[1]));
            }
            // check if dir
            if ("--dir".equalsIgnoreCase(args[i])) {
                conf.setDir(args[++i]);
            }
            // check if dbfilename is set
            if ("--dbfilename".equalsIgnoreCase(args[i])) {
                conf.setDbFilename(args[++i]);
            }
            // check if AOF enabled
            if ("--appendonly".equalsIgnoreCase(args[i])) {
                conf.setAppendOnlyEnabled(args[++i].equalsIgnoreCase("yes"));
            }
            // AOF append subdir
            if ("--appenddirname".equalsIgnoreCase(args[i])) {
                conf.setAppendDirName(args[++i]);
            }
            // AOF file
            if ("--appendfilename".equalsIgnoreCase(args[i])) {
                conf.setAppendFilename(args[++i]);
            }
            // AOF freq
            if ("--appendfsync".equalsIgnoreCase(args[i])) {
                conf.setAppendFSync(AofPersistenceManager.FsyncFrequency.valueOf(args[++i].toUpperCase()));
            }
        }

        return conf;
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

    public Path getRdbFilenamePath() {
        if (dir == null || dbFilename == null) {
            return null;
        }
        return Path.of(dir, dbFilename);
    }

    public String getDir() {
        return dir;
    }

    public String getDbFilename() {
        return dbFilename;
    }

    public boolean isAppendOnlyEnabled() {
        return appendOnlyEnabled;
    }

    public RedisServerConfiguration setPort(int port) {
        this.port = port;
        return this;
    }

    public RedisServerConfiguration setMasterHost(String masterHost) {
        this.masterHost = masterHost;
        return this;
    }

    public RedisServerConfiguration setMasterPort(Integer masterPort) {
        this.masterPort = masterPort;
        return this;
    }

    public RedisServerConfiguration setDir(String dir) {
        this.dir = dir;
        return this;
    }

    public RedisServerConfiguration setDbFilename(String dbFilename) {
        this.dbFilename = dbFilename;
        return this;
    }

    public RedisServerConfiguration setAppendOnlyEnabled(boolean appendOnlyEnabled) {
        this.appendOnlyEnabled = appendOnlyEnabled;
        return this;
    }

    public String getAppendDirName() {
        return appendDirName;
    }

    public RedisServerConfiguration setAppendDirName(String appendDirName) {
        this.appendDirName = appendDirName;
        return this;
    }

    public String getAppendFilename() {
        return appendFilename;
    }

    public RedisServerConfiguration setAppendFilename(String appendFilename) {
        this.appendFilename = appendFilename;
        return this;
    }

    public RedisServerConfiguration setAppendFSync(AofPersistenceManager.FsyncFrequency appendFSync) {
        this.appendFSync = appendFSync;
        return this;
    }

    public AofPersistenceManager.FsyncFrequency getAppendFSync() {
        return appendFSync;
    }
}
