package commands;

import commands.impl.*;
import commands.impl.geospatial.GeoAddCommand;
import commands.impl.geospatial.GeoDistCommand;
import commands.impl.geospatial.GeoPosCommand;
import commands.impl.geospatial.GeoSearchCommand;
import commands.impl.list.*;
import commands.impl.pubsub.SubscribeCommand;
import commands.impl.replication.InfoCommand;
import commands.impl.replication.PsyncCommand;
import commands.impl.replication.ReplConfCommand;
import commands.impl.replication.WaitCommand;
import commands.impl.sortedsets.*;
import commands.impl.transaction.DiscardCommand;
import commands.impl.transaction.ExecCommand;
import commands.impl.transaction.IncrCommand;
import commands.impl.transaction.MultiCommand;
import logger.Logger;

/**
 * CommandRouter is responsible for routing incoming command names to their corresponding Command implementations.
 * It acts as a factory that creates instances of Command based on the command name provided.
 */
public class CommandRouter {

    private final static Command[] COMMANDS = {
            // Basic commands
            new PingCommand(), new EchoCommand(), new ConfigCommand(),
            // Set & Get commands
            new SetCommand(), new GetCommand(), new KeysCommand(),
            // List commands
            new RpushCommand(), new LrangeCommand(), new LpushCommand(), new LlenCommand(), new LpopCommand(), new BlpopCommand(),
            // Stream commands
            new TypeCommand(),
            // Transaction commands
            new IncrCommand(), new MultiCommand(), new ExecCommand(), new DiscardCommand(),
            // Replication commands
            new InfoCommand(), new ReplConfCommand(), new PsyncCommand(), new WaitCommand(),
            // Sorted sets
            new ZAddCommand(), new ZRankCommand(), new ZRangeCommand(), new ZCardCommand(), new ZScoreCommand(), new ZRemCommand(),
            // Geospatial commands
            new GeoAddCommand(), new GeoPosCommand(), new GeoDistCommand(), new GeoSearchCommand(),
            // Pub/Sub commands
            new SubscribeCommand()
    };

    /**
     * Returns the Command instance corresponding to the given command name.
     * @param commandName the name of the command (e.g., "PING")
     * @return the Command instance for the specified command name
     * @throws IllegalArgumentException if the command name is not recognized or supported
     */
    public static Command getCommand(String commandName) throws IllegalArgumentException {
        Logger.info("Getting command: " + commandName);

        for (Command command : COMMANDS) {
            if (command.matches(commandName)) {
                Logger.info("Found command: " + command.getClass().getSimpleName());
                return command;
            }
        }

        throw new IllegalArgumentException("No command found for name: " + commandName);
    }

}
