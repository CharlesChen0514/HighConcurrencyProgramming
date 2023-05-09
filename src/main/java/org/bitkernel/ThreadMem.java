package org.bitkernel;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

public class ThreadMem {
    @Getter
    private final MessageDigest md = Task.getMessageDigestInstance();
    @Getter
    private final ByteBuffer buffer = ByteBuffer.allocate(32);
}
