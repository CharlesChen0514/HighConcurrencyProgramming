package org.bitkernel;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

@Slf4j
public class ThreadMem {
    @Getter
    private final MessageDigest md = Task.getMessageDigestInstance();
    @Getter
    private final ByteBuffer sha256Buf = ByteBuffer.allocate(32);
}
