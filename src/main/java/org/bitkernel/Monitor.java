package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import sun.java2d.d3d.D3DDrawImage;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class Monitor {
    @Getter
    private static final int UDP_PORT = 25524;
    private final String recordDir;
    private final Udp udp;
    private long generatorTaskNum;
    private long executorTaskNum;
    private long collectorTaskNum;
    private int correctTaskNum;
    private int incorrectTaskNum;

    public static void main(String[] args) {
        Monitor m = new Monitor();
        m.start();
    }

    public Monitor() {
        udp = new Udp(UDP_PORT);
        recordDir = System.getProperty("user.dir") + File.separator
                + "monitor" + File.separator + getTime() + File.separator;
        FileUtil.createFolder(recordDir);
        generatorTaskNum = 0L;
        executorTaskNum = 0L;
        collectorTaskNum = 0L;
        correctTaskNum = 0;
        incorrectTaskNum = 0;
    }

    public static String getTime() {
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("HH-mm-ss");
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
            logger.debug("Receive packet: {}", pktString);
            record(pktString);
        }
    }

    private void record(@NotNull String pktString) {
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

    private double tps(long taskNum, String time) {
        int t = Integer.parseInt(time);
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
        long newTaskNum = Long.parseLong(msg);

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("---------- %s minute ----------%n", time));
        sb.append(String.format("New task number: %d%n", newTaskNum));
        sb.append(String.format("Current TPS: %.2f%n", tps(newTaskNum)));
        executorTaskNum += newTaskNum;
        sb.append(String.format("Total task number: %d%n", executorTaskNum));
        sb.append(String.format("Average TPS: %.2f%n", tps(executorTaskNum, time)));
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
        sb.append(String.format("New task number: %d%n", newTaskNum));
        sb.append(String.format("Correct task number: %d%n", correct));
        sb.append(String.format("Incorrect task number: %d%n", incorrect));
        sb.append(String.format("Current TPS: %.2f%n", tps(newTaskNum)));
        collectorTaskNum += newTaskNum;
        correctTaskNum += correct;
        incorrectTaskNum += incorrect;
        sb.append(String.format("Total task number: %d%n", collectorTaskNum));
        sb.append(String.format("Average accuracy: %.2f%%%n", getAccuracy()));
        sb.append(String.format("Average tps: %.2f%n", tps(collectorTaskNum, time)));
        sb.append(System.lineSeparator());

        String path = recordDir + "collector";
        FileUtil.appendToFile(path, sb.toString());
    }

    private double getAccuracy() {
        return (correctTaskNum * 1.0) / (correctTaskNum + incorrectTaskNum);
    }
}
