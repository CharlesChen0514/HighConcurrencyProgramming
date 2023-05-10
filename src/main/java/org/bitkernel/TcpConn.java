package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

@Slf4j
public class TcpConn {
    @Getter
    private final Socket socket;
    @Getter
    private DataOutputStream dout;
    @Getter
    private DataInputStream din;

    public TcpConn(@NotNull Socket socket) {
        this.socket = socket;
        try {
            dout = new DataOutputStream(socket.getOutputStream());
            din = new DataInputStream(socket.getInputStream());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public TcpConn(@NotNull String ip, int port) throws IOException {
        this(new Socket(ip, port));
    }

    public void readFully(@NotNull ByteBuffer readBuffer) {
        try {
            din.readFully(readBuffer.array());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public void writeFully(@NotNull ByteBuffer writeBuffer) {
        try {
            dout.write(writeBuffer.array());
            dout.flush();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static boolean checkPort(int port) {
        try (Socket socket = new Socket()) {
            socket.bind(new InetSocketAddress(port));
            logger.debug("Tcp port {} is available", port);
            return true;
        } catch (Exception e) {
            logger.debug("Tcp port {} is unavailable", port);
            return false;
        }
    }
}
