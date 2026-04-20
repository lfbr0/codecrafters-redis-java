package commands;

import serdes.RedisSerializer;

import java.io.OutputStream;

/**
 * This classes holds the response in bytes to command - ready to send to client
 */
public class CommandResponse {

    /**
     * In some situations, we want to send the client additional data after sending response to command
     * This allows that.
     */
    @FunctionalInterface
    public interface CommandPostResponseCallback {
        void postResponse(OutputStream clientOutputStream) throws Exception;
    }

    private final byte[] responseBytes;
    private final CommandPostResponseCallback postResponseCallback;
    private final boolean sendEvenIfSlave; // send this response even if a slave

    public CommandResponse(byte[] responseBytes) {
        this.responseBytes = responseBytes;
        this.postResponseCallback = null;
        this.sendEvenIfSlave = false;
    }

    public CommandResponse(byte[] responseBytes, CommandPostResponseCallback postResponseCallback) {
        this.responseBytes = responseBytes;
        this.postResponseCallback = postResponseCallback;
        this.sendEvenIfSlave = false;
    }

    public CommandResponse(byte[] responseBytes, boolean sendEvenIfSlave) {
        this.responseBytes = responseBytes;
        this.postResponseCallback = null;
        this.sendEvenIfSlave = sendEvenIfSlave;
    }

    public static CommandResponse queued() {
        return new CommandResponse(RedisSerializer.simpleString("QUEUED"));
    }

    public static CommandResponse ok() {
        return new CommandResponse(RedisSerializer.okString());
    }

    public static CommandResponse integer(int value) {
        return new CommandResponse(RedisSerializer.integer(value));
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    public CommandPostResponseCallback getPostResponseCallback() {
        return postResponseCallback;
    }

    public boolean isSendEvenIfSlave() {
        return sendEvenIfSlave;
    }

    @Override
    public String toString() {
        return "CommandResponse{" + new String(responseBytes) + '}';
    }
}
