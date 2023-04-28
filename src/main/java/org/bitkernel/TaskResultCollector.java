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
    private final static int SAMPLE_NUM = 100;
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();
    private final static String RECORD_DIR = System.getProperty("user.dir") + File.separator + "sample" + File.separator;
    private TcpConn executorConn;
    private Udp udp;
    @Setter
    private String monitorIp;
    private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService sampleExecutor = Executors.newSingleThreadScheduledExecutor();
    private int sampleTimes = 0;

    public static void main(String[] args) {
        TaskResultCollector collector = new TaskResultCollector();
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        collector.setMonitorIp(sc.next());
        collector.init();
        collector.start();
    }

    private void init() {
        udp = new Udp();
        try (ServerSocket server = new ServerSocket(TCP_PORT)) {
            logger.debug("Waiting for executor to connect");
            Socket accept = server.accept();
            executorConn = new TcpConn(accept);
            logger.debug("Successfully connected with the executor");
            sampleExecutor.scheduleAtFixedRate(this::sample, 0, 1, TimeUnit.MINUTES);
            FileUtil.createFolder(RECORD_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void sample() {
        Set<Object> sampleSet = new HashSet<>();
        Object[] array = queue.toArray();
        int taskNum = array.length;
        queue.clear();
        while (sampleSet.size() < SAMPLE_NUM) {
            int idx = random.nextInt(taskNum);
            sampleSet.add(array[idx]);
        }
        Map<String, Boolean> resMap = sampleVerification(sampleSet);
        int[] arr = recordSampleRes(resMap);
        String monitorData = "3:" + taskNum + ":" + arr[0] + ":" + arr[1];
        udp.send(monitorIp, Monitor.getUDP_PORT(), monitorData);
    }

    private int[] recordSampleRes(@NotNull Map<String, Boolean> resMap) {
        StringBuilder rightSb = new StringBuilder();
        StringBuilder errorSb = new StringBuilder();
        int rightCount = 0;
        for (Map.Entry<String, Boolean> entry : resMap.entrySet()) {
            if (entry.getValue()) {
                rightCount++;
                rightSb.append(entry.getKey()).append(System.lineSeparator());
            } else {
                errorSb.append(entry.getKey()).append(System.lineSeparator());
            }
        }

        String path = RECORD_DIR + sampleTimes;
        sampleTimes++;
        String content = String.format("Total of %d tasks calculated correctly.%n", rightCount);
        content += rightSb.toString();
        content += String.format("Total of %d tasks calculated error.%n", SAMPLE_NUM - rightCount);
        content += errorSb.toString();
        FileUtil.write(path, content);
        return new int[]{rightCount, SAMPLE_NUM - rightCount};
    }

    @NotNull
    private Map<String, Boolean> sampleVerification(@NotNull Set<Object> sampleSet) {
        Map<String, Boolean> resMap = new HashMap<>();
        for (Object taskObj : sampleSet) {
            String taskString = (String) taskObj;
            String[] split = taskString.split(":");
            int x = Integer.parseInt(split[1]);
            int y = Integer.parseInt(split[2]);
            String res = TaskExecutor.executeTask(x, y);
            if (res.equals(split[3])) {
                resMap.put(taskString, true);
            } else {
                resMap.put(taskString, false);
            }
        }
        return resMap;
    }

    private void start() {
        while (true) {
            try {
                String taskRes = executorConn.getDin().readUTF();
                queue.offer(taskRes);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
