package org.bitkernel;

import lombok.extern.slf4j.Slf4j;

import java.security.MessageDigest;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class LimitTest {
    public static void main(String[] args) {
        long c = 0;
        byte[] buffer = new byte[8];
        MessageDigest md = Task.getMessageDigestInstance();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long time = 0;
        logger.debug("Start time: {}", System.currentTimeMillis());
        while (true) {
            long start = System.nanoTime();
            int x = random.nextInt(65535);
            int y = random.nextInt(65535);
            Task.executeTask(md, buffer, x, y);
            c++;
            if (time > 60L * 1000 * 1000000) {
                break;
            }
            long end = System.nanoTime();
            time += (end - start);
        }

        long tps = c / 60;
        logger.info("The TPS of a single CPU is: {}", tps);
        int processors = Runtime.getRuntime().availableProcessors();
        logger.info("The TPS of {} CPU is: {}", processors, tps * processors);
        logger.debug("End time: {}", System.currentTimeMillis());
    }
}
