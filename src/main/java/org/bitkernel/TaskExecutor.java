package org.bitkernel;

import com.sun.istack.internal.NotNull;
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

    /** The number of tasks a thread needs to run at one time */
    @Getter
    private static final int RUN_BATCH_SIZE = 2000;

    /** Queue size in thread pool */
    private static final int QUEUE_SIZE = 800 * 10000;
    private final ThreadPoolExecutor executeThreadPool;
    private final ExecutorPool executorPool;

    private final static int READ_BUFFER_SIZE = RUN_BATCH_SIZE * TaskGenerator.getTASK_LEN();
    private final static int WRITE_BUFFER_SIZE = RUN_BATCH_SIZE * TOTAL_TASK_LEN;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE * 5);

    private final Udp udp = new Udp();

    private final String monitorIp;
    private final String collectorIp;

    private TcpConn generatorConn;
    private TcpConn collectorConn;

    /** Number of tasks completed */
    private final LongAdder completedTaskNum = new LongAdder();

    private int minutes = 0;
    private final ThreadLocal<ThreadMem> threadLocal = ThreadLocal.withInitial(ThreadMem::new);

    private final BlockingQueue<BatchTaskExecutor> transferWaitingQueue;

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

    private TaskExecutor(@NotNull String monitorIp,
                         @NotNull String collectorIp) {
        logger.debug("Initialize the task executor");
        this.monitorIp = monitorIp;
        this.collectorIp = collectorIp;

        int processors = Runtime.getRuntime().availableProcessors();
        int threadNum;
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            threadNum = processors - 1;
        } else {
            threadNum = processors + 1;
        }
        executeThreadPool = new ThreadPoolExecutor(threadNum, threadNum, 0,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE), new ThreadPoolExecutor.DiscardPolicy());
        logger.debug("The maximum number of threads is set to {}", threadNum);
        int executorSize = threadNum * 512;
        executorPool = new ExecutorPool(executorSize);
        transferWaitingQueue = new LinkedBlockingQueue<>(executorSize);

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
                executeThreadPool.getQueue().size() * RUN_BATCH_SIZE);
        completedTaskNum.reset();
        udp.send(monitorIp, Monitor.getUDP_PORT(), message);
        minutes += 1;
    }

    private void start() {
        logger.debug("Start task executor service");
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::reportToMonitor, 0, 1, TimeUnit.MINUTES);
        executeThreadPool.execute(this::transfer);

        while (true) {
            generatorConn.readFully(readBuffer);
            BatchTaskExecutor executor = executorPool.borrow();
            if (executor == null) {
                logger.error("Executor is null");
                continue;
            }
            executor.readTask(readBuffer);
            readBuffer.clear();
            executeThreadPool.execute(executor);
        }
    }

    private void transfer() {
        while (true) {
            BatchTaskExecutor executor = null;
            try {
                executor = transferWaitingQueue.take();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
            if (executor == null) {
                continue;
            }
            writeBuffer.put(executor.getBuffer().array());
            executor.getBuffer().clear();
            executorPool.turnBack(executor);
            if (writeBuffer.position() == writeBuffer.limit()) {
                collectorConn.writeFully(writeBuffer);
                writeBuffer.clear();
            }
        }
    }

    class ExecutorPool {
        private final BlockingQueue<BatchTaskExecutor> executorQueue;

        public ExecutorPool(int size) {
            executorQueue = new LinkedBlockingQueue<>(size);
            for (int i = 0; i < size; i++) {
                executorQueue.offer(new BatchTaskExecutor());
            }
        }

        public BatchTaskExecutor borrow() {
            try {
                return executorQueue.take();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
            return null;
        }

        public void turnBack(@NotNull BatchTaskExecutor executor) {
            executorQueue.offer(executor);
        }
    }

    class BatchTaskExecutor implements Runnable {
        private ThreadMem threadMem;
        private final Task[] tasks = new Task[RUN_BATCH_SIZE];
        @Getter
        private ByteBuffer buffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE);

        public BatchTaskExecutor() {
            for (int i = 0; i < RUN_BATCH_SIZE; i++) {
                tasks[i] = new Task();
            }
        }

        public void readTask(@NotNull ByteBuffer readBuffer) {
            for (int i = 0; i < RUN_BATCH_SIZE; i++) {
                long id = readBuffer.getLong();
                int x = readBuffer.getShort() & 0xffff;
                int y = readBuffer.getShort() & 0xffff;

                tasks[i].setId(id);
                tasks[i].setX(x);
                tasks[i].setY(y);
            }
        }

        @Override
        public void run() {
            threadMem = threadLocal.get();
            for (Task task : tasks) {
                byte[] res = Task.execute(threadMem.getMd(), threadMem.getSha256Buf(), task);
                buffer.putLong(task.getId());
                buffer.putShort((short) (task.getX() & 0xffff));
                buffer.putShort((short) (task.getY() & 0xffff));
                buffer.put(res);
                threadMem.getSha256Buf().clear();
            }

            completedTaskNum.add(RUN_BATCH_SIZE);
            transferWaitingQueue.add(this);
        }
    }
}
