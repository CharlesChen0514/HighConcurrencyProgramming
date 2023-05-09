package org.bitkernel;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

@Slf4j
public class ThreadMem {
    @Getter
    private final MessageDigest md = Task.getMessageDigestInstance();
    @Getter
    private final ByteBuffer sha256Buf = ByteBuffer.allocate(32);
    @Getter
    private final Task[] tasks;
    @Getter
    private final ByteBuffer readBuffer;
    private final int capacity;
    private int idx;

    public ThreadMem(int capacity) {
        this.capacity = capacity;
        tasks = new Task[capacity];
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new Task();
        }
        readBuffer = ByteBuffer.allocate(capacity * TaskGenerator.getTASK_LEN());
    }

    public void put(long id, int x, int y) {
        if (idx == capacity) {
            logger.error("Reach capacity, please reset");
            return;
        }
        tasks[idx].setId(id);
        tasks[idx].setX(x);
        tasks[idx].setY(y);
        idx += 1;
    }

    public Task getLast() {
        if (idx == 0) {
            logger.error("It is empty, please put element first");
            return null;
        }
        return tasks[idx - 1];
    }

    public int size() {
        return idx;
    }

    public void reset() {
        idx = 0;
    }

    public boolean isEmpty() {
        return size() == 0;
    }
}
