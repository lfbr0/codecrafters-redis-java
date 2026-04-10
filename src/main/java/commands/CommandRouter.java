package commands;

import commands.impl.*;
import logger.Logger;

/**
 * CommandRouter is responsible for routing incoming command names to their corresponding Command implementations.
 * It acts as a factory that creates instances of Command based on the command name provided.
 */
public class CommandRouter {

    private final static Command[] COMMANDS = {
            // Basic commands
            new PingCommand(), new EchoCommand(),
            // Set & Get commands
            new SetCommand(), new GetCommand(),
            // List commands
            new RpushCommand(), new LrangeCommand(), new LpushCommand(), new LlenCommand(), new LpopCommand(), new BlpopCommand(),
            // Stream commands
            new TypeCommand(),
            // Transaction commands
            new IncrCommand(), new MultiCommand(), new ExecCommand(), new DiscardCommand(),
            // Replication commands
            new InfoCommand(), new ReplConfCommand(), new PsyncCommand(), new WaitCommand()
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
