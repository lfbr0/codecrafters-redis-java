package commands;

import serdes.RedisMessage;

import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class CommandContext {

    private final OutputStream outputStream;
    private final List<RedisMessage> arguments;

    public CommandContext(OutputStream outputStream, List<RedisMessage> arguments) {
        this.outputStream = outputStream;
        this.arguments = arguments;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public List<RedisMessage> getArguments() {
        return arguments == null ? null : List.copyOf(arguments);
    }
}
