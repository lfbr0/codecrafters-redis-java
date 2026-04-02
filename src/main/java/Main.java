import logger.Logger;
import server.RedisServer;
import server.RedisServerConfiguration;

public class Main {
  public static void main(String[] args) {

    try (RedisServer redisServer = new RedisServer()) {
      redisServer.start(RedisServerConfiguration.defaultConfiguration());
    } catch (Exception e) {
      Logger.error("Error starting Redis server: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

}
