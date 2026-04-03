import logger.Logger;

import java.util.Arrays;

public class Main {
  public static void main(String[] args) {

    Logger.info("Starting Redis Server with arguments: " + Arrays.toString(args));
    try (RedisServer redisServer = new RedisServer()) {
      redisServer.start(RedisServerConfiguration.args(args));
    } catch (Exception e) {
      Logger.error("Error starting Redis server: {}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

}
