package commands;

import serdes.RedisSerializer;

public class CommandResponse {

    private final byte[] responseBytes;

    public CommandResponse(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    public static CommandResponse queued() {
        return new CommandResponse(RedisSerializer.okString());
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

}
