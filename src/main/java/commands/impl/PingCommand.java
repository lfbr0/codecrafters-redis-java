package commands.impl;

import commands.Command;
import commands.CommandContext;

import java.io.IOException;

public class PingCommand implements Command {
    @Override
    public void execute(CommandContext context) throws IOException {
        // Do nothing for now, just respond to PING with PONG
        context.getOutputStream().write("+PONG\r\n".getBytes());
    }

    @Override
    public boolean matches(String commandName) {
        return "ping".equalsIgnoreCase(commandName);
    }
}
