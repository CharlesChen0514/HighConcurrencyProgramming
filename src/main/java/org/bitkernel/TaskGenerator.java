package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TaskGenerator {
    private static final int RANGE = 65535;
    @Getter
    private final static int BATCH_SIZE = 32;
    /**
     * Performance much faster than Random class
     */
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final StringBuilder sb = new StringBuilder();
    private final StopWatch stopWatch = new StopWatch();
    private final Udp udp = new Udp();
    private final String monitorIp;
    private final String executorIp;
    private final long targetTps;
    private final long targetTpm;
    private TcpConn executorConn;
    private int minutes = 0;
    private long totalTaskNum = 0;
    private long newTaskNum = 0;

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
        ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor();
        scheduledService.scheduleAtFixedRate(this::scheduled, 0, 1, TimeUnit.MINUTES);
    }

    /**
     * Scheduled tasks
     */
    private void scheduled() {
        telemetry();
        minutes += 1;
        transferTask();
    }

    /**
     * Report message to the monitor
     */
    private void telemetry() {
        String monitorData = String.format("%d@%d@%s", 0, minutes, newTaskNum);
        newTaskNum = 0;
        udp.send(monitorIp, Monitor.getUDP_PORT(), monitorData);
    }

    /**
     * Transfer {@link #targetTpm} number of tasks to the executor
     */
    private void transferTask() {
        stopWatch.start(String.valueOf(minutes));
        for (long start = totalTaskNum + 1; start < totalTaskNum + targetTpm; start += BATCH_SIZE) {
            for (int offset = 0; offset < BATCH_SIZE; offset++) {
                sb.append(generateTask(start + offset)).append("@");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(System.lineSeparator());
            try {
                executorConn.getBw().write(sb.toString());
                executorConn.getBw().flush();
                sb.setLength(0);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        totalTaskNum += targetTpm;
        newTaskNum = targetTpm;
        stopWatch.stop();
        logger.debug("Generate the {}th minute task takes {} ms", minutes, stopWatch.getLastTaskTimeMillis());
    }

    @NotNull
    private String generateTask(long id) {
        StringBuilder sb = new StringBuilder();
        int x = random.nextInt(RANGE);
        int y = random.nextInt(RANGE);
        sb.append(id).append(" ").append(x).append(" ").append(y);
        return sb.toString();
    }
}
