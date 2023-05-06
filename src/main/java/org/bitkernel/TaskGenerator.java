package org.bitkernel;

import com.sun.istack.internal.NotNull;
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
    /**
     * Performance much faster than Random class
     */
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private long totalTaskNum = 0;
    private long newTaskNum = 0;
    private final Udp udp;
    private final String monitorIp;
    private final String executorIp;
    private final long targetTps;
    private final long targetTpm;
    private TcpConn executorConn;
    private final StopWatch stopWatch;
    private int minutes;
    private final ScheduledExecutorService scheduledService = Executors.newSingleThreadScheduledExecutor();

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
        minutes = 0;
        udp = new Udp();
        stopWatch = new StopWatch();
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
        scheduledService.scheduleAtFixedRate(this::scheduled, 0, 1, TimeUnit.MINUTES);
    }

    private void scheduled() {
        telemetry();
        minutes += 1;
        generateTask();
    }

    private void telemetry() {
        String monitorData = String.format("%d@%d@%s", 0, minutes, newTaskNum);
        newTaskNum = 0;
        udp.send(monitorIp, Monitor.getUDP_PORT(), monitorData);
    }

    private void generateTask() {
        stopWatch.start(String.valueOf(minutes));
        for (long id = totalTaskNum + 1; id < totalTaskNum + targetTpm; id++) {
            int x = random.nextInt(RANGE);
            int y = random.nextInt(RANGE);
            try {
                String str = id + " " + x + " " + y + System.lineSeparator();
                executorConn.getBw().write(str);
                executorConn.getBw().flush();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        totalTaskNum += targetTpm;
        newTaskNum = targetTpm;
        stopWatch.stop();
        logger.debug("Generate the {}th minute task takes {} ms", minutes, stopWatch.getLastTaskTimeMillis());
    }
}
