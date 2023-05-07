package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

@Slf4j
public class TaskExecutor {
    @Getter
    private static final int TCP_PORT = 25522;
    private static final int QUEUE_SIZE = 800 * 10000;
    @Getter
    private static final int TASK_LEN = 12 + 32;
    private final ScheduledExecutorService scheduledThreadPool = new ScheduledThreadPoolExecutor(2);
    private final ThreadPoolExecutor executeThreadPool;
    private final Udp udp = new Udp();
    private final ConcurrentLinkedQueue<Task> taskQueue = new ConcurrentLinkedQueue<>();
    private final ByteBuffer readBuffer = ByteBuffer.allocate(TaskGenerator.getBUFFER_SIZE());
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(TaskGenerator.getBATCH_SIZE() * TASK_LEN);
    private final String monitorIp;
    private final String collectorIp;
    private TcpConn generatorConn;
    private TcpConn collectorConn;
    private long completedTaskNum = 0;
    private int minutes = 0;

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
        int processors = Runtime.getRuntime().availableProcessors();
//        threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(processors + 1);
        executeThreadPool = new ThreadPoolExecutor(processors - 1, processors - 1, 0,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE), new ThreadPoolExecutor.DiscardPolicy());
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
        long curCompletedTaskCount = executeThreadPool.getCompletedTaskCount() * TaskGenerator.getBATCH_SIZE();
        long newTaskCount = curCompletedTaskCount - completedTaskNum;
        String message = String.format("%d@%d@%d %d", 1, minutes, newTaskCount,
                executeThreadPool.getQueue().size() * TaskGenerator.getBATCH_SIZE());
        completedTaskNum = curCompletedTaskCount;
        udp.send(monitorIp, Monitor.getUDP_PORT(), message);
        minutes += 1;
    }

    private void transfer() {
        int c = 0;
        while (true) {
            if (taskQueue.isEmpty()) {
                continue;
            }
            Task task = taskQueue.poll();
            writeBuffer.putLong(task.getId());
            writeBuffer.putShort((short) (task.getX() & 0xffff));
            writeBuffer.putShort((short) (task.getY() & 0xffff));
            writeBuffer.put(task.getRes());
            c += 1;
            if (c == TaskGenerator.getBATCH_SIZE()) {
                try {
                    collectorConn.getDout().write(writeBuffer.array());
                    collectorConn.getDout().flush();
//                    logger.debug("Send success");
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
                c = 0;
                writeBuffer.clear();
            }
        }
    }

    private void start() {
        logger.debug("Start task executor service");
        scheduledThreadPool.scheduleAtFixedRate(this::reportToMonitor, 0, 1, TimeUnit.MINUTES);
//        scheduledThreadPool.scheduleAtFixedRate(() -> {
//            try {
//                collectorConn.getBw().flush();
//            } catch (IOException e) {
//                logger.error(e.getMessage());
//            }
//        }, 0, 1, TimeUnit.SECONDS);
        executeThreadPool.submit(this::transfer);

        while (true) {
            try {
                generatorConn.getDin().read(readBuffer.array());
                BatchTask batchTask = new BatchTask(readBuffer);
                executeThreadPool.submit(batchTask);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    class BatchTask implements Runnable {
        private final List<Task> taskList = new ArrayList<>();

        public BatchTask(@NotNull ByteBuffer buffer) {
            for (int i = 0; i < TaskGenerator.getBATCH_SIZE(); i++) {
                long id = buffer.getLong();
                int x = buffer.getShort() & 0xffff;
                int y = buffer.getShort() & 0xffff;
                taskList.add(new Task(id, x, y));
            }
            buffer.clear();
        }

        @Override
        public void run() {
            for (Task task : taskList) {
                byte[] res = Task.execute(task);
                task.setRes(res);
                taskQueue.offer(task);
            }
        }
    }
}
