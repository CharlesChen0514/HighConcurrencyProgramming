package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class TaskResultCollector {
    @Getter
    private final static int TCP_PORT = 25523;
    @Getter
    private final static int SAMPLE_NUM = 100;
    private final static double SAMPLE_PCT = 0.005;
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();
    private final String RECORD_DIR;
    private final TcpConn executorConn;
    private final Udp udp;
    private final String monitorIp;
    private final ConcurrentLinkedQueue<String> sampleQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
    private final LongAdder count = new LongAdder();
    private int minutes = 0;

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        String monitorIp = sc.next();
        TaskResultCollector collector = new TaskResultCollector(monitorIp);
        collector.start();
    }

    private TaskResultCollector(@NotNull String monitorIp) {
        this.monitorIp = monitorIp;
        udp = new Udp();
        RECORD_DIR = System.getProperty("user.dir") + File.separator + "sample" + File.separator
                + Monitor.getTime() + File.separator;
        try (ServerSocket server = new ServerSocket(TCP_PORT)) {
            logger.debug("Waiting for executor to connect");
            Socket accept = server.accept();
            executorConn = new TcpConn(accept);
            logger.debug("Successfully connected with the executor");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void scheduled() {
        logger.debug("Execute scheduled jobs");
        if (minutes == 0) {
            telemetry(0, 0, 0);
        } else {
            Map<String, Boolean> resMap = sampleVerification();
            int rightCount = rightSampleNum(resMap);
            recordSampleRes(resMap, rightCount);
            telemetry(count.longValue(), rightCount, SAMPLE_NUM - rightCount);
            count.reset();
        }
        minutes += 1;
        logger.debug("Execute scheduled jobs down");
    }

    private int rightSampleNum(@NotNull Map<String, Boolean> resMap) {
        int rightCount = 0;
        for (Map.Entry<String, Boolean> entry : resMap.entrySet()) {
            if (entry.getValue()) {
                rightCount++;
            }
        }
        return rightCount;
    }

    private void telemetry(long taskNum, int rightCount, int errorCount) {
        String monitorData = String.format("%d@%d@%d %d %d", 2, minutes, taskNum, rightCount, errorCount);
        logger.debug(String.format("The %dth minute, task number %d, correct sample number %d, error sample number %d",
                minutes, taskNum, rightCount, errorCount));
        udp.send(monitorIp, Monitor.getUDP_PORT(), monitorData);
    }

    private void recordSampleRes(@NotNull Map<String, Boolean> resMap, int rightCount) {
        StringBuilder rightSb = new StringBuilder();
        StringBuilder errorSb = new StringBuilder();
        for (Map.Entry<String, Boolean> entry : resMap.entrySet()) {
            if (entry.getValue()) {
                rightSb.append(entry.getKey()).append(System.lineSeparator());
            } else {
                errorSb.append(entry.getKey()).append(System.lineSeparator());
            }
        }

        String path = RECORD_DIR + minutes;
        String content = String.format("Total of %d tasks calculated correctly, accuracy %.2f%% %n",
                rightCount, rightCount * 100.0 / SAMPLE_NUM);
        content += rightSb + System.lineSeparator();
        content += String.format("Total of %d tasks calculated error.%n", SAMPLE_NUM - rightCount);
        content += errorSb.toString();
        FileUtil.write(path, content);
    }

    @NotNull
    private Map<String, Boolean> sampleVerification() {
        Map<String, Boolean> resMap = new HashMap<>();
        Object[] array = sampleQueue.toArray();
        sampleQueue.clear();
        for (Object taskObj : array) {
            String taskString = (String) taskObj;
            String[] split = taskString.split(" ");
            int x = Integer.parseInt(split[1].trim());
            int y = Integer.parseInt(split[2].trim());
            String res = TaskUtil.executeTask(x, y);
            if (res.equals(split[3])) {
                resMap.put(taskString, true);
            } else {
                resMap.put(taskString, false);
            }
        }
        return resMap;
    }

    private void randomSample(@NotNull String taskListString) {
        String[] split = taskListString.split("@");
        for (int i = 0; i < split.length && sampleQueue.size() < SAMPLE_NUM; i++) {
            if (random.nextDouble() <= SAMPLE_PCT) {
                sampleQueue.add(split[i]);
            }
        }
    }

    private boolean isNeedSample() {
        return sampleQueue.size() < SAMPLE_NUM && random.nextDouble() <= SAMPLE_PCT;
    }

    private void start() {
        FileUtil.createFolder(RECORD_DIR);
        scheduled.scheduleAtFixedRate(this::scheduled, 0, 1, TimeUnit.MINUTES);
        for (;;) {
            try {
                String taskString = executorConn.getBr().readLine();
                if (taskString == null) {
                    continue;
                }
                if (sampleQueue.size() < SAMPLE_NUM && random.nextDouble() <= SAMPLE_PCT) {
                    sampleQueue.add(taskString);
                }
                count.increment();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
