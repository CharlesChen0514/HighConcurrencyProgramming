package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class TaskGenerator {
    private static final int RANGE = 65535;
    /**
     * Performance much faster than Random class
     */
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private long taskNum = 0;
    private final Udp udp;
    private final String monitorIp;
    private final String executorIp;
    private final long targetTps;
    private final long targetTpm;
    private TcpConn executorConn;
    private final StopWatch stopWatch;
    private int minutes;

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
            logger.error("Cannot connect to the executor");
        }
        logger.debug("Initialize the task generator done");
    }

    public void start() {
        logger.debug("Start task generator");
        while (true) {
            minutes += 1;
            logger.debug("Start generating tasks for the {} minute", minutes);
            long round = 60 * 1000;
            stopWatch.start(String.valueOf(minutes));
            generateTask();
            stopWatch.stop();
            long taskGenerateTime = stopWatch.getLastTaskTimeMillis();
            logger.debug("Success generating tasks for the {} minute, consuming {} ms",
                    minutes, taskGenerateTime);
            round -= taskGenerateTime;
            logger.debug("Program goes to sleep: {} ms", round);
            try {
                Thread.sleep(round);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
            logger.debug("Program wake up");
            String monitorData = String.format("%d@%d@%s", 0, minutes, targetTpm);
            udp.send(monitorIp, Monitor.getUDP_PORT(), monitorData);
        }
    }

    private void generateTask() {
        long c = 0;
        while (c < targetTpm) {
            c++;
            int x = random.nextInt(RANGE);
            int y = random.nextInt(RANGE);
            // 传输给 executor
        }
        taskNum += targetTpm;
    }
}
