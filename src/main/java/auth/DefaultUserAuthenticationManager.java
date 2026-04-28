package auth;

import logger.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultUserAuthenticationManager {

    private static DefaultUserAuthenticationManager INSTANCE;
    private final AtomicReference<String> defaultHash = new AtomicReference<>(null);

    public static synchronized DefaultUserAuthenticationManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DefaultUserAuthenticationManager();
        }
        return INSTANCE;
    }

    public void setPassword(String defaultUserPassword) {
        try {
            byte[] encodedHash = MessageDigest
                    .getInstance("SHA-256")
                    .digest(defaultUserPassword.getBytes(StandardCharsets.UTF_8));

            StringBuilder hexString = new StringBuilder(2 * encodedHash.length);
            for (int i = 0; i < encodedHash.length; i++) {
                String hex = Integer.toHexString(0xff & encodedHash[i]);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            defaultHash.set(hexString.toString());
        } catch (Exception ex) {
            Logger.error("Default User Auth Manager - Failed to set password", ex);
        }
    }

    public Optional<String> getPasswordHash() {
        return Optional.ofNullable(defaultHash.get());
    }

}
