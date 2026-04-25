package data;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class AofPersistenceManager {

    private static AofPersistenceManager INSTANCE;
    private final Path dirPath;
    private boolean isEnabled = false;
    private String appendDir = "appendonlydir";
    private String appendFilename = "appendonly.aof";
    private FsyncFrequency appendFsync = FsyncFrequency.EVERYSEC;

    public AofPersistenceManager(Path dirPath, boolean isEnabled) {
        this.dirPath = dirPath;
        this.isEnabled = isEnabled;
    }

    public static synchronized AofPersistenceManager init(Path dbFilenamePath, boolean isEnabled) {
        if (INSTANCE == null) {
            INSTANCE = new AofPersistenceManager(dbFilenamePath, isEnabled);
        }
        return INSTANCE;
    }

    public synchronized static AofPersistenceManager getInstance() {
        if (INSTANCE == null) {
            return init(Paths.get("./"), false); // if not initialized, not enabled - dummy instance
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
        EVERYSEC
    }

}
