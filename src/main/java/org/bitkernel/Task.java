package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class Task {
    @Getter
    @Setter
    private long id;
    @Getter
    @Setter
    private int x;
    @Getter
    @Setter
    private int y;

    @NotNull
    public static byte[] execute(@NotNull MessageDigest md, @NotNull ByteBuffer buffer,
                                 @NotNull Task task) {
        return Task.executeTask(md, buffer, task.x, task.y);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(" ").append(x).append(" ").append(y);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Task && toString().equals(o.toString());
    }

    public static MessageDigest getMessageDigestInstance() {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            logger.error(e.getMessage());
        }
        return md;
    }

    @NotNull
    public static byte[] executeTask(@NotNull MessageDigest md,
                                     @NotNull ByteBuffer buffer,
                                     int x, int y) {
        long pow = myPow(x, y);
        buffer.putLong(pow);
        byte[] array = buffer.array();
        for (int i = 0; i < 10; i++) {
            array = md.digest(array);
        }
        md.reset();
        return array;
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
