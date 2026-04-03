package commands;

public interface Command {
    CommandResponse execute(CommandContext context) throws Exception;
    boolean matches(String commandName);
}
