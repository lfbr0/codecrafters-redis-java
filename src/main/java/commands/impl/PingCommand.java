package commands.impl;

import commands.Command;
import commands.CommandContext;

public class PingCommand implements Command {
    @Override
    public void execute(CommandContext context) {

    }

    @Override
    public boolean matches(String commandName) {
        return "ping".equalsIgnoreCase(commandName);
    }
}
