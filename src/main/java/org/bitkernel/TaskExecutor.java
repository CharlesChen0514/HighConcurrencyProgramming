package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StopWatch;

import javax.xml.bind.DatatypeConverter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Scanner;

@Slf4j
public class TaskExecutor {
    @Getter
    private static final int TCP_PORT = 25522;
    private static MessageDigest md;
    private TcpConn generatorConn;
    private TcpConn collectorConn;
    private Udp udp;
    @Setter
    private String monitorIp;
    @Setter
    private String collectorIp;

    static {
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
    }

    public static void main(String[] args) {
        TaskExecutor taskExecutor = new TaskExecutor();
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        taskExecutor.setMonitorIp(sc.next());
        System.out.print("Please input the collector ip: ");
        taskExecutor.setCollectorIp(sc.next());
        taskExecutor.init();
    }

    private void init() {
        logger.debug("Initialize the task executor");
        try (ServerSocket server = new ServerSocket(TCP_PORT)) {
            collectorConn = new TcpConn(collectorIp, TaskResultCollector.getTCP_PORT());
            logger.debug("Successfully connected with the collector");
            logger.debug("Waiting for generator to connect");
            Socket accept = server.accept();
            generatorConn = new TcpConn(accept);
            logger.debug("Successfully connected with the generator");
        } catch (Exception e) {
            logger.error("Cannot connect to the collector");
        }
        logger.debug("Initialize the task executor done");
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

    public static long myPow(int x, int n) {
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
}
