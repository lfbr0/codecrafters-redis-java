package commands;

import serdes.RedisSerializer;

import java.io.OutputStream;

public class CommandResponse {

    @FunctionalInterface
    public interface CommandPostResponseCallback {
        void postResponse(OutputStream clientOutputStream) throws Exception;
    }

    private final byte[] responseBytes;
    private final CommandPostResponseCallback postResponseCallback;

    public CommandResponse(byte[] responseBytes) {
        this.responseBytes = responseBytes;
        this.postResponseCallback = null;
    }

    public CommandResponse(byte[] responseBytes, CommandPostResponseCallback postResponseCallback) {
        this.responseBytes = responseBytes;
        this.postResponseCallback = postResponseCallback;
    }

    public static CommandResponse queued() {
        return new CommandResponse(RedisSerializer.simpleString("QUEUED"));
    }

    public static CommandResponse ok() {
        return new CommandResponse(RedisSerializer.okString());
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    public CommandPostResponseCallback getPostResponseCallback() {
        return postResponseCallback;
    }

    @Override
    public String toString() {
        return "CommandResponse{" + new String(responseBytes) + '}';
    }
}
