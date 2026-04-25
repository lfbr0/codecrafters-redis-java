package data;

import logger.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class AofPersistenceManager {

    private static AofPersistenceManager INSTANCE;
    private final Path dirPath;
    private final boolean isEnabled;
    private final String appendDir;
    private final String appendFilename;
    private final FsyncFrequency appendFsync;

    public AofPersistenceManager(Path dirPath,
                                 boolean isEnabled,
                                 String appendDir,
                                 String appendFilename,
                                 FsyncFrequency appendFsync) {
        this.dirPath = dirPath != null ? dirPath : Paths.get("./").toAbsolutePath().normalize();
        this.isEnabled = isEnabled;
        this.appendDir = appendDir != null ? appendDir : "appendonlydir";
        this.appendFilename = appendFilename != null ? appendFilename : "appendonly.aof";
        this.appendFsync = appendFsync != null ? appendFsync : FsyncFrequency.EVERYSEC;
    }

    public static synchronized AofPersistenceManager init(Path dirPath,
                                                          boolean isEnabled,
                                                          String appendDir,
                                                          String appendFilename,
                                                          FsyncFrequency appendFsync) {
        if (INSTANCE == null) {
            INSTANCE = new AofPersistenceManager(dirPath, isEnabled, appendDir, appendFilename, appendFsync);
        }

        Logger.info("AOF - Created append dir=" + Paths
                .get(INSTANCE.getDir(), INSTANCE.getAppendDir()).toFile().mkdirs());

        return INSTANCE;
    }

    public synchronized static AofPersistenceManager getInstance() {
        if (INSTANCE == null) {
            return init(Paths.get("./"), false,
                    null, null, null); // if not initialized, not enabled - dummy instance
        }
        return INSTANCE;
    }

    public boolean isEnabled() {
        return isEnabled;
    }

    public String getDir() {
        return dirPath.normalize().toFile().getAbsolutePath();
    }

    /**
     * The subdirectory under dir where AOF and manifest files are stored
     * @return subdirectory under dir where AOF and manifest files are stored
     */
    public String getAppendDir() {
        return appendDir;
    }

    /**
     * The name of the append-only file that records write operations
     * @return name of the append-only file that records write operations
     */
    public String getAppendFilename() {
        return appendFilename;
    }

    /**
     * How often buffered writes are flushed to the AOF file on disk
     * @return how often buffered writes are flushed to the AOF file on disk
     */
    public AofPersistenceManager.FsyncFrequency getAppendFSync() {
        return appendFsync;
    }

    public enum FsyncFrequency {
        EVERYSEC, ALWAYS
    }

}
