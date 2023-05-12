package org.bitkernel;

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
    private static final String HEX = "0123456789abcdef";

    
    public static byte[] execute( MessageDigest md,  byte[] buffer,
                                  Task task) {
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

    
    public static byte[] executeTask( MessageDigest md,
                                      byte[] buffer,
                                     int x, int y) {
        long pow = myPow(x, y);
        for (int j = 0; j < 8; j++) {
            buffer[j] = (byte) (pow >> j * 8);
        }
        for (int i = 0; i < 10; i++) {
            buffer = md.digest(buffer);
        }
//        md.reset();
        return buffer;
    }

    public static long myPow(long x, long y) {
        long ans = 1;
        while (y > 0) {
            if ((y & 1) == 1) ans *= x;
            x = x * x;
            y = y >> 1;
        }
        return ans;
    }

    public static String getSha256String(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(HEX.charAt((b >> 4) & 0x0f));
            sb.append(HEX.charAt(b & 0x0f));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
    }
}
