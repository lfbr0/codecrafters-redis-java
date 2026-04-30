package auth;

import logger.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultUserAuthenticationManager {

    private static DefaultUserAuthenticationManager INSTANCE;
    private final AtomicReference<String> defaultHash = new AtomicReference<>(null);
    private final Set<UUID> authenticatedUserIds = new ConcurrentSkipListSet<>();

    public static synchronized DefaultUserAuthenticationManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DefaultUserAuthenticationManager();
        }
        return INSTANCE;
    }

    public void setPassword(String defaultUserPassword) {
        try {
            defaultHash.set(computeHash(defaultUserPassword));
        } catch (Exception ex) {
            Logger.error("Default User Auth Manager - Failed to set password", ex);
        }
    }

    private String computeHash(String defaultUserPassword) {
        byte[] encodedHash = null;
        try {
            encodedHash = MessageDigest
                    .getInstance("SHA-256")
                    .digest(defaultUserPassword.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // this is literally never going to happen
            throw new RuntimeException(e);
        }

        StringBuilder hexString = new StringBuilder(2 * encodedHash.length);
        for (int i = 0; i < encodedHash.length; i++) {
            String hex = Integer.toHexString(0xff & encodedHash[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    public Optional<String> getPasswordHash() {
        return Optional.ofNullable(defaultHash.get());
    }

    public boolean tryAuthenticate(String password, UUID clientUUID) {
        boolean passwordMatches = getPasswordHash()
                .map(hash -> hash.equals( computeHash(password) ))
                .orElse(false);

        if (passwordMatches)
            authenticatedUserIds.add(clientUUID);

        return  passwordMatches;
    }

    public boolean userIsAuthenticated(UUID clientUUID) {
        // if no pass, then add this to auth'd users & let him pass
        String currentHash = defaultHash.get();
        if (currentHash == null) {
            authenticatedUserIds.add(clientUUID);
            return true;
        }
        return authenticatedUserIds.contains(clientUUID);
    }
}
