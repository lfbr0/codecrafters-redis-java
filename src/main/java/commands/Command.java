package commands;

public interface Command {
    void execute(CommandContext context);
    boolean matches(String commandName);
}
