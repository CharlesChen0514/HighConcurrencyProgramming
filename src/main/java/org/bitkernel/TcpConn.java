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
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    public TcpConn(@NotNull Socket socket) {
        this.socket = socket;
        try {
            dout = new DataOutputStream(socket.getOutputStream());
            din = new DataInputStream(socket.getInputStream());
            bis = new BufferedInputStream(socket.getInputStream());
            bos = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public TcpConn(@NotNull String ip, int port) throws IOException {
        this(new Socket(ip, port));
    }

    public synchronized void readFully(@NotNull ByteBuffer readBuffer) {
        try {
            din.readFully(readBuffer.array());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

//    public synchronized void read(@NotNull ByteBuffer readBuffer) {
//        int curLen = 0;
//        int capacity = readBuffer.limit();
//        while (curLen < capacity) {
//            try {
//                curLen += din.read(readBuffer.array(), curLen, capacity - curLen);
//            } catch (IOException e) {
//               logger.error(e.getMessage());
//            }
//        }
//    }
//
//    public synchronized void write(@NotNull ByteBuffer writeBuffer) {
//        try {
//            dout.write(writeBuffer.array());
//            dout.flush();
//        } catch (IOException e) {
//            logger.error(e.getMessage());
//        }
//    }

    public synchronized void read(@NotNull ByteBuffer readBuffer) {
        int curLen = 0;
        int capacity = readBuffer.limit();
        while (curLen < capacity) {
            try {
                curLen += bis.read(readBuffer.array(), curLen, capacity - curLen);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public synchronized void write(@NotNull ByteBuffer writeBuffer) {
        try {
            bos.write(writeBuffer.array());
//            dout.flush();
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
