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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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

    private final Udp udp = new Udp();
    private final String monitorIp;
    private final TcpConn executorConn;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(TaskExecutor.getRUN_BATCH_SIZE() * TaskExecutor.getTOTAL_TASK_LEN());

    /** Number of tasks received in one minute */
    private final LongAdder taskNum = new LongAdder();
    private int minutes = 0;
    private final ThreadMem threadMem = new ThreadMem();
    private final Map<Task, byte[]> sampleMap = new LinkedHashMap<>();
    private final byte[][] resBuffer = new byte[SAMPLE_NUM][32];
    private int bufferId = 0;

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

        Map<Task, Boolean> verificationMap = sampleVerification();
        int rightCount = rightSampleNum(verificationMap);
        recordSampleRes(verificationMap, rightCount);
        telemetry(taskNum.longValue(), rightCount, SAMPLE_NUM - rightCount);

        taskNum.reset();
        minutes += 1;
        sampleMap.clear();
        bufferId = 0;
        logger.debug("Execute scheduled jobs down");
    }

    private int rightSampleNum(@NotNull Map<Task, Boolean> verificationMap) {
        int rightCount = 0;
        for (Map.Entry<Task, Boolean> entry : verificationMap.entrySet()) {
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

    private void recordSampleRes(@NotNull Map<Task, Boolean> verficationMap, int rightCount) {
        StringBuilder rightSb = new StringBuilder();
        StringBuilder errorSb = new StringBuilder();
        for (Map.Entry<Task, Boolean> entry : verficationMap.entrySet()) {
            Task task = entry.getKey();
            byte[] res = sampleMap.get(task);
            if (entry.getValue()) {
                rightSb.append(task).append(" ").append(DatatypeConverter.printHexBinary(res)).append(System.lineSeparator());
            } else {
                errorSb.append(task).append(" ").append(DatatypeConverter.printHexBinary(res)).append(System.lineSeparator());
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
    private Map<Task, Boolean> sampleVerification() {
        Map<Task, Boolean> verificationMap = new HashMap<>();
        if (sampleMap.isEmpty()) {
            logger.error("Sample queue is empty, something error, please check.");
            return verificationMap;
        }

        for (Task task : sampleMap.keySet()) {
            byte[] res1 = sampleMap.get(task);
            byte[] res2 = Task.execute(threadMem.getMd(), threadMem.getSha256Buf(), task);
            threadMem.getSha256Buf().clear();
            String res1Str = DatatypeConverter.printHexBinary(res1);
            String res2Str = DatatypeConverter.printHexBinary(res2);
            verificationMap.put(task, res1Str.equals(res2Str));
//            logger.debug(task.toString() + " " + res1Str + " " + verificationMap.get(taskPair));
        }
        return verificationMap;
    }

    private boolean isNeedSample() {
        return sampleMap.size() < SAMPLE_NUM && random.nextDouble() <= SAMPLE_PCT;
    }

    private void start() {
        FileUtil.createFolder(recordDir);
        ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
        scheduled.scheduleAtFixedRate(this::scheduled, 0, 1, TimeUnit.MINUTES);

        while (true) {
            executorConn.readFully(readBuffer);
            while (readBuffer.position() < readBuffer.limit()) {
                if (isNeedSample()) {
                    long id = readBuffer.getLong();
                    int x = readBuffer.getShort() & 0xffff;
                    int y = readBuffer.getShort() & 0xffff;

                    byte[] res = resBuffer[bufferId];
                    readBuffer.get(res);
                    sampleMap.put(new Task(id, x, y), res);
                    bufferId += 1;
                } else {
                    readBuffer.position(readBuffer.position() + TaskExecutor.getTOTAL_TASK_LEN());
                }
            }
            readBuffer.clear();
            taskNum.add(TaskExecutor.getRUN_BATCH_SIZE());
        }
    }
}
