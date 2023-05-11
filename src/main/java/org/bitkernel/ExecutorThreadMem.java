package org.bitkernel;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

@Slf4j
public class ExecutorThreadMem {
    @Getter
    private final MessageDigest md = Task.getMessageDigestInstance();
    @Getter
    private final ByteBuffer sha256Buf = ByteBuffer.allocate(32);
    @Getter
    private final Task[] tasks = new Task[TaskGenerator.getBATCH_SIZE()];
    private final int READ_BUFFER_SIZE = TaskGenerator.getBATCH_SIZE() * TaskGenerator.getTASK_LEN();
    private final int WRITE_BUFFER_SIZE = TaskGenerator.getBATCH_SIZE() * TaskExecutor.getTOTAL_TASK_LEN();
    @Getter
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    @Getter
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE);
    private final int capacity = TaskGenerator.getBATCH_SIZE();
    @Getter
    private int idx;

    public ExecutorThreadMem() {
        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new Task();
        }
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
