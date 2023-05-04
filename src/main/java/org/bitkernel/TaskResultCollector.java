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

@Slf4j
public class TaskResultCollector {
    @Getter
    private final static int TCP_PORT = 25523;
    @Getter
    private final static int SAMPLE_NUM = 100;
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();
    private final String RECORD_DIR;
    private final TcpConn executorConn;
    private final Udp udp;
    @Setter
    private String monitorIp;
    private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
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
        Object[] array;
        synchronized (queue) {
            array = queue.toArray();
            queue.clear();
        }
        if (array.length == 0) {
            telemetry(0, 0, 0);
        } else {
            Set<Object> samples = sample(array);
            Map<String, Boolean> resMap = sampleVerification(samples);
            int rightCount = rightSampleNum(resMap);
            recordSampleRes(resMap, rightCount);
            telemetry(array.length, rightCount, SAMPLE_NUM - rightCount);
        }
        minutes += 1;
        logger.debug("Execute scheduled jobs down");
    }

    private Set<Object> sample(@NotNull Object[] array) {
        Set<Object> sampleSet = new HashSet<>();
        int taskNum = array.length;
        while (sampleSet.size() < SAMPLE_NUM) {
            int idx = random.nextInt(taskNum);
            sampleSet.add(array[idx]);
        }
        return sampleSet;
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

    private void telemetry(int taskNum, int rightCount, int errorCount) {
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
    private Map<String, Boolean> sampleVerification(@NotNull Set<Object> sampleSet) {
        Map<String, Boolean> resMap = new HashMap<>();
        for (Object taskObj : sampleSet) {
            String taskString = (String) taskObj;
            String[] split = taskString.split(" ");
            int x = Integer.parseInt(split[0].trim());
            int y = Integer.parseInt(split[1].trim());
            String res = TaskExecutor.executeTask(x, y);
            if (res.equals(split[2])) {
                resMap.put(taskString, true);
            } else {
                resMap.put(taskString, false);
            }
        }
        return resMap;
    }

    private void start() {
        FileUtil.createFolder(RECORD_DIR);
        scheduled.scheduleAtFixedRate(this::scheduled, 0, 1, TimeUnit.MINUTES);
        while (true) {
            try {
                String taskRes = executorConn.getDin().readLine();
                if (taskRes != null) {
                    queue.offer(taskRes);
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
