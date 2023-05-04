package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TaskExecutor {
    @Getter
    private static final int TCP_PORT = 25522;
    private TcpConn generatorConn;
    private TcpConn collectorConn;
    private final Udp udp;
    private final String monitorIp;
    private final String collectorIp;
    private volatile long completedTaskNum;
    public ExecutorService threadPool;
    private final ScheduledExecutorService telemetry = Executors.newSingleThreadScheduledExecutor();
    private int minutes;

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        String monitorIp = sc.next();
        System.out.print("Please input the collector ip: ");
        String collectorIp = sc.next();
        TaskExecutor taskExecutor = new TaskExecutor(monitorIp, collectorIp);
        taskExecutor.start();
    }

    private TaskExecutor(@NotNull String monitorIp,
                         @NotNull String collectorIp) {
        logger.debug("Initialize the task executor");
        this.monitorIp = monitorIp;
        this.collectorIp = collectorIp;
        completedTaskNum = 0;
        minutes = 0;
        udp = new Udp();
        threadPool = Executors.newFixedThreadPool(10);
        try (ServerSocket server = new ServerSocket(TCP_PORT)) {
            logger.debug("Waiting for generator to connect");
            Socket accept = server.accept();
            generatorConn = new TcpConn(accept);
            logger.debug("Successfully connected with the generator");
            collectorConn = new TcpConn(collectorIp, TaskResultCollector.getTCP_PORT());
            logger.debug("Successfully connected with the collector");
        } catch (Exception e) {
            logger.error("Cannot connect to the collector, please start collector server first");
            System.exit(-1);
        }
        logger.debug("Initialize the task executor done");
    }

    private synchronized void incrementCompletedTaskNum() {
        completedTaskNum += 1;
    }

    private void reportToMonitor() {
        String message = String.format("%d@%d@%d", 1, minutes, completedTaskNum);
        udp.send(monitorIp, Monitor.getUDP_PORT(), message);
        minutes += 1;
        completedTaskNum = 0;
    }

    private void start() {
        logger.debug("Start task executor service");
        telemetry.scheduleAtFixedRate(this::reportToMonitor, 0, 1, TimeUnit.MINUTES);
        while (true) {
            try {
                String task = generatorConn.getDin().readUTF();
                String[] split = task.split(" ");
                int x = Integer.parseInt(split[0]);
                int y = Integer.parseInt(split[1]);
                threadPool.submit(new Task(x, y));
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @NotNull
    public static String executeTask(int x, int y) {
        long pow = myPow(x, y);
        String res = String.valueOf(pow);
        for (int i = 0; i < 10; i++) {
            res = SHA256(res);
        }
        return res;
    }

    public static long myPow(long x, long n) {
        long N = n;
        return N >= 0 ? quickMul(x, N) : 1L / quickMul(x, -N);
    }

    public static long quickMul(long x, long N) {
        long ans = 1L;
        // 贡献的初始值为 x
        long x_contribute = x;
        // 在对 N 进行二进制拆分的同时计算答案
        while (N > 0) {
            if (N % 2 == 1) {
                // 如果 N 二进制表示的最低位为 1，那么需要计入贡献
                ans *= x_contribute;
            }
            // 将贡献不断地平方
            x_contribute *= x_contribute;
            // 舍弃 N 二进制表示的最低位，这样我们每次只要判断最低位即可
            N /= 2;
        }
        return ans;
    }

    @NotNull
    public static String SHA256(@NotNull String data) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
        assert md != null;
        byte[] digest = md.digest(data.getBytes(StandardCharsets.UTF_8));
        return DatatypeConverter.printHexBinary(digest).toLowerCase();
    }

    public static void testSHA256() throws NoSuchAlgorithmException {
        String password = "SHA-256";
        StopWatch stop = new StopWatch();
        stop.start();
        for (int i = 0; i < 10000000; i++) {
            String v = SHA256(password);
        }
        stop.stop();
        System.out.println(stop.getTotalTimeMillis());
    }

    public static void testPow() {
        StopWatch stop = new StopWatch();
        stop.start();
        for (int i = 0; i < 100000000; i++) {
            double v = myPow(65535, 65535);
        }
        stop.stop();
        System.out.println(stop.getTotalTimeMillis());
    }

    @AllArgsConstructor
    class Task implements Runnable {
        private int x;
        private int y;

        @Override
        public void run() {
            String res = executeTask(x, y);
            try {
                String pktString = String.format("%d %d %s%n", x, y, res);
                collectorConn.getDout().write(pktString.getBytes());
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            incrementCompletedTaskNum();
        }
    }
}
