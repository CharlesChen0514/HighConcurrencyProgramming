package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
public class TaskUtil {
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
}
