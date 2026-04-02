import lombok.extern.slf4j.Slf4j;
import server.RedisServer;
import server.RedisServerConfiguration;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

@Slf4j
public class Main {
  public static void main(String[] args) {

    try (RedisServer redisServer = new RedisServer()) {
      redisServer.start(RedisServerConfiguration.defaultConfiguration());
    } catch (Exception e) {
      log.error("Error starting Redis server: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

}
