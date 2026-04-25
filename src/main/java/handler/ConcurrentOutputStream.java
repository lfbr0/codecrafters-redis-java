package handler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentOutputStream extends OutputStream {
    private final OutputStream protectedOutputStream;
    private final ReentrantReadWriteLock.WriteLock lock;

    public ConcurrentOutputStream(OutputStream outputStream) {
        this.protectedOutputStream = outputStream;
        this.lock = new ReentrantReadWriteLock().writeLock();
    }

    @Override
    public void write(int i) throws IOException {
        try {
            lock.lock();
            protectedOutputStream.write(i);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        try {
            lock.lock();
            protectedOutputStream.write(b);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            lock.lock();
            protectedOutputStream.write(b, off, len);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            lock.lock();
            protectedOutputStream.flush();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            lock.lock();
            protectedOutputStream.close();
        } finally {
            lock.unlock();
        }
    }
}
