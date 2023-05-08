package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
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

    /** Byte size of the task include the result */
    @Getter
    private static final int TOTAL_TASK_LEN = TaskGenerator.getTASK_LEN() + 32;

    /** The number of tasks a thread needs to run at one time */
    private static final int RUN_BATCH_SIZE = 1000;

    /** Queue size in thread pool */
    private static final int QUEUE_SIZE = 800 * 10000;
    private final ThreadPoolExecutor executeThreadPool;

    private final Udp udp = new Udp();

    /** Cache queue for tasks, added to the queue when task execution is complete */
    private final ConcurrentLinkedQueue<Task> taskQueue = new ConcurrentLinkedQueue<>();

    private final ByteBuffer readBuffer = ByteBuffer.allocate(RUN_BATCH_SIZE * TaskGenerator.getTASK_LEN());
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(RUN_BATCH_SIZE * TOTAL_TASK_LEN);

    private final String monitorIp;
    private final String collectorIp;

    private TcpConn generatorConn;
    private TcpConn collectorConn;

    /** Number of tasks completed */
    private long completedTaskNum = 0;

    private int minutes = 0;

    public static void main(String[] args) {
        if (!TaskExecutor.test()) {
            System.exit(-1);
        }
        Scanner sc = new Scanner(System.in);
        System.out.print("Please input the monitor ip: ");
        String monitorIp = sc.next();
        System.out.print("Please input the collector ip: ");
        String collectorIp = sc.next();
        logger.debug(String.format("Monitor ip: %s, collector ip:%s", monitorIp, collectorIp));

        TaskExecutor taskExecutor = new TaskExecutor(monitorIp, collectorIp);
        taskExecutor.start();
    }

    private TaskExecutor(@NotNull String monitorIp,
                         @NotNull String collectorIp) {
        logger.debug("Initialize the task executor");
        this.monitorIp = monitorIp;
        this.collectorIp = collectorIp;

        int processors = Runtime.getRuntime().availableProcessors();
        executeThreadPool = new ThreadPoolExecutor(processors + 1, processors + 1, 0,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE), new ThreadPoolExecutor.DiscardPolicy());
        logger.debug("The maximum number of threads is set to {}", processors + 1);
        logger.debug("Endian is {}", readBuffer.order());

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
        long curCompletedTaskCount = executeThreadPool.getCompletedTaskCount() * RUN_BATCH_SIZE;
        long newTaskCount = curCompletedTaskCount - completedTaskNum;
        String message = String.format("%d@%d@%d %d", 1, minutes, newTaskCount,
                executeThreadPool.getQueue().size() * RUN_BATCH_SIZE);
        completedTaskNum = curCompletedTaskCount;
        udp.send(monitorIp, Monitor.getUDP_PORT(), message);
        minutes += 1;
    }

    private static boolean test() {
        int bytes = Long.BYTES + Short.BYTES * 2 + Task.executeTask(0, 0).length;
        if (bytes != TOTAL_TASK_LEN) {
            logger.error("The task bytes size [{}] does not match the expected size [{}]", bytes, TOTAL_TASK_LEN);
        } else {
            logger.debug("The task bytes size match the expected size [{}]", TOTAL_TASK_LEN);
        }
        return bytes == TOTAL_TASK_LEN;
    }

    private void transfer() {
        int c = 0;
        while (true) {
            if (taskQueue.isEmpty()) {
                continue;
            }
            if (writeBuffer.position() + TOTAL_TASK_LEN > writeBuffer.limit()) {
                logger.error("Exceeded buffer size limit: {}, {}",
                        writeBuffer.position() + TOTAL_TASK_LEN, writeBuffer.limit());
                break;
            }
            Task task = taskQueue.poll();
            writeBuffer.putLong(task.getId());
            writeBuffer.putShort((short) (task.getX() & 0xffff));
            writeBuffer.putShort((short) (task.getY() & 0xffff));
            writeBuffer.put(task.getRes());

            c += 1;
            if (c == RUN_BATCH_SIZE) {
                try {
                    collectorConn.getDout().write(writeBuffer.array());
                    collectorConn.getDout().flush();
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
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::reportToMonitor, 0, 1, TimeUnit.MINUTES);
        executeThreadPool.submit(this::transfer);

        while (true) {
            try {
                generatorConn.getDin().read(readBuffer.array());
                BatchTask batchTask = new BatchTask(readBuffer);
                readBuffer.clear();
                executeThreadPool.submit(batchTask);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    class BatchTask implements Runnable {
        private final List<Task> taskList = new ArrayList<>();

        public BatchTask(@NotNull ByteBuffer buffer) {
            for (int i = 0; i < RUN_BATCH_SIZE; i++) {
                long id = buffer.getLong();
                int x = buffer.getShort() & 0xffff;
                int y = buffer.getShort() & 0xffff;
                taskList.add(new Task(id, x, y));
            }
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
