package data;

import java.nio.file.Path;
import java.util.Optional;

public class PersistenceManager {

    private static PersistenceManager INSTANCE;
    private final Path dbFilenamePath;

    public PersistenceManager(Path dbFilenamePath) {
        this.dbFilenamePath = dbFilenamePath;
    }

    public static synchronized PersistenceManager init(Path dbFilenamePath) {
        if (INSTANCE == null) {
            INSTANCE = new PersistenceManager(dbFilenamePath);
        }
        return INSTANCE;
    }

    public synchronized static Optional<PersistenceManager> getInstance() {
        return Optional.ofNullable(INSTANCE);
    }

    public String getDbFileDir() {
        return dbFilenamePath.toFile().getParent();
    }

    public String getDbFilename() {
        return dbFilenamePath.toFile().getName();
    }

    public void readFromDbFile() {
        // TODO
    }

}
