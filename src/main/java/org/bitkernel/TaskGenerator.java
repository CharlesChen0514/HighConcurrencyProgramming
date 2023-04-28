package org.bitkernel;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class TaskGenerator {
    private static final int RANGE = 65535;
    /** Performance much faster than Random class */
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private long seqId = 0;
    private Udp udp;
    @Setter
    private String monitorIp;
    @Setter
    private String executorIp;
    private TcpConn executorConn;
    public static void main(String[] args) {
        TaskGenerator taskGenerator = new TaskGenerator();
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        taskGenerator.setMonitorIp(sc.next());
        System.out.print("Please input the executor ip: ");
        taskGenerator.setExecutorIp(sc.next());
        taskGenerator.init();
    }

    public void init() {
        logger.debug("Initialize the task generator");
        try {
            executorConn = new TcpConn(executorIp, TaskExecutor.getTCP_PORT());
        } catch (Exception e) {
            logger.error("Cannot connect to the executor");
        }
        logger.debug("Initialize the task generator done");
    }
}
