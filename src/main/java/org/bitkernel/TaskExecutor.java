package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;
import java.util.concurrent.*;

@Slf4j
public class TaskExecutor {
    @Getter
    private static final int TCP_PORT = 25522;
    private TcpConn generatorConn;
    private TcpConn collectorConn;
    private final Udp udp;
    private final String monitorIp;
    private final String collectorIp;
    private long completedTaskNum;
    public ThreadPoolExecutor threadPool;
    private final ScheduledExecutorService telemetry = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService flush = Executors.newSingleThreadScheduledExecutor();
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
        int processors = Runtime.getRuntime().availableProcessors();
//        threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(processors + 1);
        threadPool = new ThreadPoolExecutor(processors + 1, processors + 1, 0,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(400 * 10000), new ThreadPoolExecutor.DiscardPolicy());
        logger.debug("The maximum number of threads is set to {}", processors + 1);
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

    private void reportToMonitor() {
        long curCompletedTaskCount = threadPool.getCompletedTaskCount();
        long newTaskCount = curCompletedTaskCount - completedTaskNum;
        String message = String.format("%d@%d@%d %d", 1, minutes, newTaskCount, threadPool.getQueue().size());
        completedTaskNum = curCompletedTaskCount;
        udp.send(monitorIp, Monitor.getUDP_PORT(), message);
        minutes += 1;
    }

    private void start() {
        logger.debug("Start task executor service");
        telemetry.scheduleAtFixedRate(this::reportToMonitor, 0, 1, TimeUnit.MINUTES);
        flush.scheduleAtFixedRate(() -> {
            try {
                collectorConn.getBw().flush();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);
        for (; ; ) {
            try {
                String task = generatorConn.getBr().readLine();
                if (task == null) {
                    continue;
                }
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
        String pow = myPow(x, y);
        byte[] bytes = pow.getBytes(StandardCharsets.UTF_8);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < 10; i++) {
                bytes = md.digest(bytes);
            }
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
        return DatatypeConverter.printHexBinary(bytes);
    }

    public static String myPow(long x, long n) {
        long ans = 1;
        long t = n;
        while (t != 0) {
            if ((t & 1) == 1) ans *= x;
            x *= x;
            t >>= 1;
        }
        return String.valueOf(ans);
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
                collectorConn.getBw().write(pktString);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
