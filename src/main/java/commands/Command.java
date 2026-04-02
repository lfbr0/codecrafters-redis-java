package commands;

public interface Command {
    void execute(CommandContext context) throws Exception;
    boolean matches(String commandName);
}
