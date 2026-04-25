package data;

import logger.Logger;
import serdes.RedisMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AofPersistenceManager {

    private static AofPersistenceManager INSTANCE;
    private final Path dirPath;
    private final boolean isEnabled;
    private final String appendDir;
    private final String appendFilename;
    private final FsyncFrequency appendFsync;

    // for operational use
    private Path writeDir;
    private File incrementalFile;
    private File manifestFile;

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

        try {
            INSTANCE.createBaseDirAndFiles();
        } catch (IOException ex) {
            Logger.error("Failed to create AOF base directory and files", ex);
        }

        return INSTANCE;
    }

    /**
     * Persists redis command message to incremental file if enabled
     * @param redisMessage command message to persist
     * @return true if persisted
     */
    public boolean persist(RedisMessage redisMessage) {
        if (!isEnabled)
            return false;

        if (incrementalFile == null) {
            Logger.error("AOF - Cannot persist, incremental file is null!");
            return false;
        }

        try {
            Files.write(incrementalFile.toPath(), redisMessage.getContentBytes());
            return true;
        } catch (IOException ex) {
            Logger.error("AOF - Failed to write to incremental file", ex);
            return false;
        }
    }

    /**
     * Creates the base directory and files for working this
     * @throws IOException
     */
    private void createBaseDirAndFiles() throws IOException {
        if (!isEnabled) return;
        this.writeDir = Paths.get(INSTANCE.getDir(), INSTANCE.getAppendDir());
        Logger.info("AOF - created write dir=" + writeDir.toFile().mkdirs());

        this.incrementalFile = Files
                .createFile(writeDir.resolve(appendFilename + ".1.incr.aof"))
                .toFile();
        Logger.info("AOF - created incremental file=" + incrementalFile.createNewFile());

        String manifestContent = String.format("file %s seq 1 type i", incrementalFile.getName());
        this.manifestFile = Files
                .writeString(writeDir.resolve(appendFilename + ".manifest"), manifestContent)
                .toFile();
        Logger.info("AOF - wrote manifest file to=" + manifestFile.getName());
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
