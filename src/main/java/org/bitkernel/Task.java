package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
@AllArgsConstructor
public class Task {
    @Getter
    private final long id;
    @Getter
    private final int x;
    @Getter
    private final int y;
    @Getter
    @Setter
    private byte[] res;

    public Task(long id, int x, int y) {
        this.id = id;
        this.x = x;
        this.y = y;
    }

    @NotNull
    public static byte[] execute(@NotNull Task task) {
        return Task.executeTask(task.x, task.y);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(" ").append(x).append(" ").append(y);
        return sb.toString();
    }

    public String detailed() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(" ").append(x).append(" ").append(y)
                .append(" ").append(DatatypeConverter.printHexBinary(res));
        return sb.toString();
    }

    @NotNull
    public static byte[] executeTask(int x, int y) {
        long pow = myPow(x, y);
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.putLong(pow);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < 10; i++) {
                md.digest(buffer.array(), 0, buffer.array().length);
            }
        } catch (NoSuchAlgorithmException | DigestException e) {
            logger.error(e.getMessage());
        }
        return buffer.array();
    }

    public static long myPow(long x, long n) {
        long ans = 1;
        long t = n;
        while (t != 0) {
            if ((t & 1) == 1) ans *= x;
            x *= x;
            t >>= 1;
        }
        return ans;
    }

    public static void main(String[] args) {
    }
}
