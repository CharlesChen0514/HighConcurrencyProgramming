package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

@Slf4j
public class Monitor {
    @Getter
    private static final int UDP_PORT = 25524;
    private final Udp udp = new Udp(UDP_PORT);

    /** Directory of the monitoring message record */
    private final String recordDir;

    /** The cumulative number of tasks for the three services */
    private long generatorTaskNum = 0L;
    private long executorTaskNum = 0L;
    private long collectorTaskNum = 0L;

    /** Accumulated number of correct and incorrect task samples */
    private int correctTaskNum = 0;
    private int incorrectTaskNum = 0;
    private final Set<String> pktStringSet = new LinkedHashSet<>();

    public static void main(String[] args) {
        Monitor m = new Monitor();
        m.start();
    }

    public Monitor() {
        recordDir = System.getProperty("user.dir") + File.separator
                + "monitor" + File.separator + getTime() + File.separator;
        FileUtil.createFolder(recordDir);
        logger.debug("The monitoring message is stored in {}", recordDir);
    }

    public static String getTime() {
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd-HH-mm-ss-SSS");
        Date date = new Date();
        return sdf.format(date);
    }

    private void start() {
        logger.debug("Start monitor server");
        while (true) {
            // 0 -> generator, 1 -> executor, 2 -> collector
            // e.g. 0@2@msg, representing the message sent by
            // generator in the second minute.
            String pktString = udp.receiveString();
            if (!pktStringSet.contains(pktString)) {
                pktStringSet.add(pktString);
                logger.debug("Receive packet: {}", pktString);
                recordMsg(pktString);
            }
        }
    }

    private void recordMsg(@NotNull String pktString) {
        String[] split = pktString.split("@");
        int code = Integer.parseInt(split[0]);
        switch (code) {
            case 0:
                recordGeneratorMsg(split[1], split[2]);
                break;
            case 1:
                recordExecutorMsg(split[1], split[2]);
                break;
            case 2:
                recordCollectorMsg(split[1], split[2]);
                break;
            default:
                logger.error("Error code: {}", code);
        }
    }

    private double tps(long taskNum, @NotNull String time) {
        int t = Integer.parseInt(time);
        if (t == 0) {
            return  0D;
        }
        return (taskNum * 1.0) / (t * 60);
    }

    private double tps(long taskNum) {
        return (taskNum * 1.0) / 60;
    }

    private void recordGeneratorMsg(@NotNull String time,
                                    @NotNull String msg) {
        long newTaskNum = Long.parseLong(msg);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("---------- %s minute ----------%n", time));
        sb.append(String.format("--- %s --%n", getTime()));

        sb.append(String.format("New task number: %d%n", newTaskNum));
        sb.append(String.format("Current TPS: %.2f%n", tps(newTaskNum)));

        generatorTaskNum += newTaskNum;
        sb.append(String.format("Total task number: %d%n", generatorTaskNum));
        sb.append(String.format("Average TPS: %.2f%n", tps(generatorTaskNum, time)));
        sb.append(System.lineSeparator());

        String path = recordDir + "generator";
        FileUtil.appendToFile(path, sb.toString());
    }

    private void recordExecutorMsg(@NotNull String time,
                                   @NotNull String msg) {
        String[] split = msg.split(" ");
        long newTaskNum = Long.parseLong(split[0]);
        int waitingQueueTaskNum = Integer.parseInt(split[1]);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("---------- %s minute ----------%n", time));
        sb.append(String.format("--- %s --%n", getTime()));

        sb.append(String.format("New task number: %d%n", newTaskNum));
        sb.append(String.format("Current TPS: %.2f%n", tps(newTaskNum)));

        executorTaskNum += newTaskNum;
        sb.append(String.format("Total task number: %d%n", executorTaskNum));
        sb.append(String.format("Average TPS: %.2f%n", tps(executorTaskNum, time)));
        sb.append(String.format("Waiting queue task number: %d%n", waitingQueueTaskNum));
        sb.append(System.lineSeparator());

        String path = recordDir + "executor";
        FileUtil.appendToFile(path, sb.toString());
    }

    private void recordCollectorMsg(@NotNull String time,
                                    @NotNull String msg) {
        String[] split = msg.split(" ");
        long newTaskNum = Long.parseLong(split[0]);
        int correct = Integer.parseInt(split[1]);
        int incorrect = Integer.parseInt(split[2]);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("---------- %s minute ----------%n", time));
        sb.append(String.format("--- %s --%n", getTime()));
        sb.append(String.format("New task number: %d%n", newTaskNum));
        sb.append(String.format("Current TPS: %.2f%n", tps(newTaskNum)));
        sb.append(String.format("Correct task number: %d%n", correct));
        sb.append(String.format("Incorrect task number: %d%n", incorrect));
        sb.append(String.format("Current accuracy: %.2f%%%n", getAccuracy(correct, incorrect)));

        collectorTaskNum += newTaskNum;
        correctTaskNum += correct;
        incorrectTaskNum += incorrect;
        sb.append(String.format("Total task number: %d%n", collectorTaskNum));
        sb.append(String.format("Average tps: %.2f%n", tps(collectorTaskNum, time)));
        sb.append(String.format("Average accuracy: %.2f%%%n", getAccuracy()));
        sb.append(System.lineSeparator());

        String path = recordDir + "collector";
        FileUtil.appendToFile(path, sb.toString());
    }

    private double getAccuracy() {
        return getAccuracy(correctTaskNum, incorrectTaskNum);
    }

    private double getAccuracy(long correct, long incorrect) {
        return correct == 0 ? 0: (correct * 100.0) / (correct + incorrect);
    }
}
