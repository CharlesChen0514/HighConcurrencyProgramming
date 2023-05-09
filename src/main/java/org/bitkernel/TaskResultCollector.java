package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class TaskResultCollector {

    private final static ThreadLocalRandom random = ThreadLocalRandom.current();
    @Getter
    private final static int TCP_PORT = 25523;

    /** Number of sample verification */
    @Getter
    private final static int SAMPLE_NUM = 100;
    /** Sampling probability */
    private final static double SAMPLE_PCT = 0.005;
    /** Directory of the sample verification result record */
    private final String recordDir;

    private final ConcurrentLinkedQueue<Task> sampleQueue = new ConcurrentLinkedQueue<>();

    private final Udp udp = new Udp();
    private final String monitorIp;
    private final TcpConn executorConn;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(TaskExecutor.getTOTAL_TASK_LEN());

    /** Number of tasks received in one minute */
    private final LongAdder taskNum = new LongAdder();
    private int minutes = 0;
    private static final MessageDigest md = Task.getMessageDigestInstance();

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        String monitorIp = sc.next();
        logger.debug(String.format("Monitor ip: %s", monitorIp));

        TaskResultCollector collector = new TaskResultCollector(monitorIp);
        collector.start();
    }

    private TaskResultCollector(@NotNull String monitorIp) {
        logger.debug("Initialize the task collector");
        this.monitorIp = monitorIp;

        recordDir = System.getProperty("user.dir") + File.separator + "sample" + File.separator
                + Monitor.getTime() + File.separator;
        logger.debug("The sampling records are stored in {}", recordDir);
        logger.debug("Endian is {}", readBuffer.order());

        try (ServerSocket server = new ServerSocket(TCP_PORT)) {
            logger.debug("Waiting for executor to connect");
            Socket accept = server.accept();
            executorConn = new TcpConn(accept);
            logger.debug("Successfully connected with the executor");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debug("Initialize the task collector done");
    }

    private void scheduled() {
        logger.debug("Execute scheduled jobs");
        if (minutes == 0) {
            telemetry(0, 0, 0);
            minutes += 1;
            return;
        }

        Map<String, Boolean> verificationMap = sampleVerification();
        int rightCount = rightSampleNum(verificationMap);
        recordSampleRes(verificationMap, rightCount);
        telemetry(taskNum.longValue(), rightCount, SAMPLE_NUM - rightCount);
        taskNum.reset();
        minutes += 1;
        logger.debug("Execute scheduled jobs down");
    }

    private int rightSampleNum(@NotNull Map<String, Boolean> verificationMap) {
        int rightCount = 0;
        for (Map.Entry<String, Boolean> entry : verificationMap.entrySet()) {
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

        String path = recordDir + minutes;
        String content = String.format("Total of %d tasks calculated correctly, accuracy %.2f%% %n",
                rightCount, rightCount * 100.0 / SAMPLE_NUM);
        content += rightSb + System.lineSeparator();
        content += String.format("Total of %d tasks calculated error.%n", SAMPLE_NUM - rightCount);
        content += errorSb.toString();
        FileUtil.write(path, content);
    }

    @NotNull
    private Map<String, Boolean> sampleVerification() {
        Map<String, Boolean> verificationMap = new HashMap<>();
        if (sampleQueue.isEmpty()) {
            logger.error("Sample queue is empty, something error, please check.");
            return verificationMap;
        }

        Object[] array = sampleQueue.toArray();
        sampleQueue.clear();
        for (Object taskObj : array) {
            Task task = (Task) taskObj;
            byte[] res2 = Task.execute(md, task);
            String res1Str = DatatypeConverter.printHexBinary(task.getRes());
            String res2Str = DatatypeConverter.printHexBinary(res2);
            verificationMap.put(task.detailed(), res1Str.equals(res2Str));
        }
        return verificationMap;
    }

    private boolean isNeedSample() {
        return sampleQueue.size() < SAMPLE_NUM && random.nextDouble() <= SAMPLE_PCT;
    }

    private void start() {
        FileUtil.createFolder(recordDir);
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        scheduled.scheduleAtFixedRate(this::scheduled, 0, 1, TimeUnit.MINUTES);

        while (true) {
            try {
                executorConn.getDin().readFully(readBuffer.array());
                if (isNeedSample()) {
                    long id = readBuffer.getLong();
                    int x = readBuffer.getShort() & 0xffff;
                    int y = readBuffer.getShort() & 0xffff;
                    byte[] res = new byte[32];
                    readBuffer.get(res);
                    Task task = new Task(id, x, y, res);
                    sampleQueue.add(task);
//                    logger.debug(task.detailed());
                }
                readBuffer.clear();
                taskNum.increment();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
