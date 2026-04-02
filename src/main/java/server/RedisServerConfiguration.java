package server;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@Builder
public class RedisServerConfiguration {

    public static final int DEFAULT_PORT = 6379;

    private final int port;

    public static RedisServerConfiguration defaultConfiguration() {
        return RedisServerConfiguration.builder()
                .port(DEFAULT_PORT)
                .build();
    }
}
