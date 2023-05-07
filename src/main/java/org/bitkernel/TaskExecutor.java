package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

@Slf4j
public class TaskExecutor {
    @Getter
    private static final int TCP_PORT = 25522;
    private static final int QUEUE_SIZE = 800 * 10000;
    private final ThreadPoolExecutor executeThreadPool;
    private final ScheduledExecutorService scheduledThreadPool;
    private final Udp udp;
    private final String monitorIp;
    private final String collectorIp;
    private final ConcurrentLinkedQueue<Task> taskQueue;
    private TcpConn generatorConn;
    private TcpConn collectorConn;
    private long completedTaskNum;
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
        taskQueue = new ConcurrentLinkedQueue<>();
        int processors = Runtime.getRuntime().availableProcessors();
//        threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(processors + 1);
        executeThreadPool = new ThreadPoolExecutor(processors + 1, processors + 1, 0,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE), new ThreadPoolExecutor.DiscardPolicy());
        logger.debug("The maximum number of threads is set to {}", processors + 1);
        scheduledThreadPool = new ScheduledThreadPoolExecutor(2);

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
        while (true) {
            if (taskQueue.isEmpty()) {
                continue;
            }
            Task task = taskQueue.poll();
            try {
                collectorConn.getBw().write(task + System.lineSeparator());
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private void start() {
        logger.debug("Start task executor service");
        scheduledThreadPool.scheduleAtFixedRate(this::reportToMonitor, 0, 1, TimeUnit.MINUTES);
        scheduledThreadPool.scheduleAtFixedRate(() -> {
            try {
                collectorConn.getBw().flush();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);
        executeThreadPool.submit(this::transfer);

        while (true) {
            try {
                String taskListString = generatorConn.getBr().readLine();
                if (taskListString == null) {
                    continue;
                }
                executeThreadPool.submit(new BatchTask(taskListString));
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    class Task {
        private final String id;
        private final int x;
        private final int y;
        @Getter
        @Setter
        private String res;

        public Task(@NotNull String taskStr) {
            String[] split = taskStr.split(" ");
            id = split[0];
            x = Integer.parseInt(split[1]);
            y = Integer.parseInt(split[2]);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(id).append(" ").append(x).append(" ").append(y).append(" ").append(res);
            return sb.toString();
        }
    }

    class BatchTask implements Runnable {
        private final List<Task> taskList = new ArrayList<>();

        public BatchTask(@NotNull String taskListString) {
            String[] split = taskListString.split("@");
            for (String str : split) {
                taskList.add(new Task(str));
            }
        }

        @Override
        public void run() {
            for (Task task : taskList) {
                String res = TaskUtil.executeTask(task.x, task.y);
                task.setRes(res);
                taskQueue.offer(task);
            }
        }
    }
}
