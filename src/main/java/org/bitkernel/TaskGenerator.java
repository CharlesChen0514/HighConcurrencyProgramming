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
    /** Size range of x and y values */
    private static final int RANGE = 65535;

    /** Byte size of the task without the result */
    @Getter
    private final static int TASK_LEN = 12;

    /** Performance much faster than Random class */
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final StopWatch stopWatch = new StopWatch();
    private final Udp udp = new Udp();

    private final String monitorIp;
    private final String executorIp;
    private final long targetTps;
    private final long targetTpm;
    private final ByteBuffer buffer;

    /** Number of tasks in one transmission */
    private final int batchSize;

    private TcpConn executorConn;
    private int minutes = 0;
    private long totalTaskNum = 0;

    /** Total number of tasks generated in a minute and the practice consumed */
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
        logger.debug(String.format("Monitor ip: %s, executor ip: %s, target TPS: %d",
                monitorIp, executorIp, targetTps));

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
        this.batchSize = (int) Math.ceil(targetTps * 1.0 / 100);
        int bufferSize = batchSize * TASK_LEN;
        buffer = ByteBuffer.allocate(bufferSize);
        logger.debug("Endian is {}", buffer.order());
        logger.debug(String.format("Target TPM: %d, batch size: %d, buffer size: %d",
                targetTpm, batchSize, bufferSize));

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
        ScheduledExecutorService telemetry = Executors.newSingleThreadScheduledExecutor();
        telemetry.scheduleAtFixedRate(this::telemetry, 0, 1, TimeUnit.MINUTES);
        logger.debug("Start scheduled thread: telemetry");

        ScheduledExecutorService transferTask = Executors.newSingleThreadScheduledExecutor();
        transferTask.scheduleAtFixedRate(this::transferTask, 0, 10, TimeUnit.MILLISECONDS);
        logger.debug("Start scheduled thread: transferTask");
    }

    /**
     * Report message to the monitor
     */
    private void telemetry() {
        logger.debug("Generate the {}th minute task takes {} ms", minutes, taskGenerateTime);
        taskGenerateTime = 0;
        logger.debug("New task number: {}", newTaskNum);
        String monitorData = String.format("%d@%d@%s", 0, minutes, newTaskNum);
        newTaskNum = 0;
        minutes += 1;
        udp.send(monitorIp, Monitor.getUDP_PORT(), monitorData);
    }

    /**
     * Transfer {@link #batchSize} number of tasks to the executor
     */
    private void transferTask() {
        stopWatch.start(String.valueOf(System.currentTimeMillis()));
        for (int offset = 0; offset < batchSize; offset++) {
            if (buffer.position() + TASK_LEN > buffer.limit()) {
                logger.error("Exceeded buffer size limit: {}, {}",
                        buffer.position() + TASK_LEN, buffer.limit());
                break;
            }
            int x = random.nextInt(RANGE) + 1;
            int y = random.nextInt(RANGE) + 1;
            buffer.putLong(totalTaskNum + offset);
            buffer.putShort((short) (x & 0xffff));
            buffer.putShort((short) (y & 0xffff));
//            logger.debug(String.format("%d %d %d", totalTaskNum + offset, x, y));
        }
        try {
            executorConn.getDout().write(buffer.array());
            executorConn.getDout().flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        buffer.clear();
        totalTaskNum += batchSize;
        newTaskNum += batchSize;
        stopWatch.stop();
        taskGenerateTime += stopWatch.getLastTaskTimeMillis();
    }
}
