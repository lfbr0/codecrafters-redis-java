package commands;

public interface Command {
    CommandResponse execute(CommandContext context) throws Exception;
    boolean matches(String commandName);

    // by default, command is not write (doesn't write into memory)
    default boolean isWriteCommand() {
        return false;
    }
}
