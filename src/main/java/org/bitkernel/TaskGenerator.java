package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.concurrent.*;

@Slf4j
public class TaskGenerator {
    private static final int RANGE = 65535;
    @Getter
    private final static int BATCH_SIZE = 1000;
    private final static int TASK_LEN = 12;
    @Getter
    private final static int BUFFER_SIZE = BATCH_SIZE * TASK_LEN;
    /**
     * Performance much faster than Random class
     */
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final StopWatch stopWatch = new StopWatch();
    private final ScheduledExecutorService scheduledThreadPool = new ScheduledThreadPoolExecutor(2);
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private final Udp udp = new Udp();
    private final String monitorIp;
    private final String executorIp;
    private final long targetTps;
    private final long targetTpm;
    private TcpConn executorConn;
    private int minutes = 0;
    private long totalTaskNum = 0;
    private long newTaskNum = 0;
    private long taskGenerateTime = 0L;

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        String monitorIp = sc.next();
        System.out.print("Please input the executor ip: ");
        String executorIp = sc.next();
        System.out.print("Please input the target TPS: ");
        long targetTps = sc.nextLong();
        TaskGenerator taskGenerator = new TaskGenerator(monitorIp, executorIp, targetTps);
        taskGenerator.start();
    }

    public TaskGenerator(@NotNull String monitorIp,
                         @NotNull String executorIp, long targetTps) {
        logger.debug("Initialize the task generator");
        this.monitorIp = monitorIp;
        this.executorIp = executorIp;
        this.targetTps = targetTps;
        this.targetTpm = targetTps * 60;

        try {
            executorConn = new TcpConn(executorIp, TaskExecutor.getTCP_PORT());
        } catch (Exception e) {
            logger.error("Cannot connect to the executor, please start executor server first");
            System.exit(-1);
        }
        logger.debug("Initialize the task generator done");
    }

    public void start() {
        logger.debug("Start task generator");
        scheduledThreadPool.scheduleAtFixedRate(this::telemetry, 0, 1, TimeUnit.MINUTES);
        scheduledThreadPool.scheduleAtFixedRate(this::transferTask, 0, 1, TimeUnit.SECONDS);
//        while (true) {
//            transferTask();
//        }
    }

    /**
     * Report message to the monitor
     */
    private void telemetry() {
        logger.debug("Generate the {}th minute task takes {} ms", minutes, taskGenerateTime);
        taskGenerateTime = 0;
        String monitorData = String.format("%d@%d@%s", 0, minutes, newTaskNum);
        newTaskNum = 0;
        minutes += 1;
        udp.send(monitorIp, Monitor.getUDP_PORT(), monitorData);
    }

    /**
     * Transfer {@link #targetTpm} number of tasks to the executor
     */
    private void transferTask() {
        stopWatch.start(String.valueOf(System.currentTimeMillis()));
        for (long start = totalTaskNum + 1; start < totalTaskNum + targetTps; start += BATCH_SIZE) {
            for (int offset = 0; offset < BATCH_SIZE; offset++) {
                buffer.putLong(start + offset);
                int x = random.nextInt(RANGE);
                int y = random.nextInt(RANGE);
                buffer.putShort((short) (x & 0xffff));
                buffer.putShort((short) (y & 0xffff));
            }
            try {
                executorConn.getDout().write(buffer.array());
                executorConn.getDout().flush();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            buffer.clear();
        }
        totalTaskNum += targetTps;
        newTaskNum += targetTps;
        stopWatch.stop();
        taskGenerateTime += stopWatch.getLastTaskTimeMillis();
        try {
            Thread.sleep(1000 - stopWatch.getLastTaskTimeMillis());
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }
}
