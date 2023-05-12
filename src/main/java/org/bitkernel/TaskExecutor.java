package org.bitkernel;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
public class TaskExecutor {
    @Getter
    private static final int TCP_PORT = 25522;

    /** Byte size of the task include the result */
    @Getter
    private static final int TOTAL_TASK_LEN = TaskGenerator.getTASK_LEN() + 32;

    /** Queue size in thread pool */
    private static final int QUEUE_SIZE = 5 * 10000;
    private final ThreadPoolExecutor executeThreadPool;

    private final Udp udp = new Udp();

    private final String monitorIp;
    private final String collectorIp;

    private TcpConn generatorConn;
    private TcpConn collectorConn;

    /** Number of tasks completed */
    private final LongAdder completedTaskNum = new LongAdder();

    private int minutes = 0;
    private final ThreadLocal<ExecutorThreadMem> threadLocal = ThreadLocal.withInitial(ExecutorThreadMem::new);

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        String monitorIp = sc.next();
        System.out.print("Please input the collector ip: ");
        String collectorIp = sc.next();
        logger.debug(String.format("Monitor ip: %s, collector ip:%s", monitorIp, collectorIp));

        TaskExecutor taskExecutor = new TaskExecutor(monitorIp, collectorIp);
        taskExecutor.start();
    }

    private TaskExecutor( String monitorIp,
                          String collectorIp) {
        logger.debug("Initialize the task executor");
        this.monitorIp = monitorIp;
        this.collectorIp = collectorIp;

        int processors = Runtime.getRuntime().availableProcessors();
        int threadNum;
        threadNum = processors + 1;
        executeThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);
        logger.debug("The maximum number of threads is set to {}", threadNum);

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
        String message = String.format("%d@%d@%d %d", 1, minutes, completedTaskNum.longValue(),
                executeThreadPool.getQueue().size() * TaskGenerator.getTASK_LEN());
        completedTaskNum.reset();
        udp.send(monitorIp, Monitor.getUDP_PORT(), message);
        minutes += 1;
    }

    private void start() {
        logger.debug("Start task executor service");
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::reportToMonitor, 0, 1, TimeUnit.MINUTES);
        for (int i = 0; i < executeThreadPool.getMaximumPoolSize(); i++) {
            executeThreadPool.execute(new Executor());
        }
    }

    class Executor implements Runnable {
        private ExecutorThreadMem executorThreadMem;

        @Override
        public void run() {
            executorThreadMem = threadLocal.get();
            while (true) {
                runBatch();
            }
        }

        private void runBatch() {
            ByteBuffer readBuffer = executorThreadMem.getReadBuffer();
            generatorConn.read(readBuffer);

            for (int i = 0; i < TaskGenerator.getBATCH_SIZE(); i++) {
                long id = readBuffer.getLong();
                int x = readBuffer.getShort() & 0xffff;
                int y = readBuffer.getShort() & 0xffff;
                executorThreadMem.put(id, x, y);
            }

            ByteBuffer writeBuffer = executorThreadMem.getWriteBuffer();
            for (Task task : executorThreadMem.getTasks()) {
                byte[] res = Task.execute(executorThreadMem.getMd(), executorThreadMem.getSha256Buf().array(), task);
                writeBuffer.putLong(task.getId());
                writeBuffer.putShort((short) (task.getX() & 0xffff));
                writeBuffer.putShort((short) (task.getY() & 0xffff));
                writeBuffer.put(res);
            }

            collectorConn.write(writeBuffer);
            writeBuffer.clear();
            readBuffer.clear();
            executorThreadMem.reset();
            completedTaskNum.add(TaskGenerator.getBATCH_SIZE());
        }
    }
}
