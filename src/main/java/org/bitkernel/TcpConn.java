package org.bitkernel;

import com.sun.istack.internal.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

@Slf4j
public class TcpConn {
    @Getter
    private final Socket socket;
    @Getter
    private BufferedReader br;
    @Getter
    private BufferedWriter bw;
    private static final int BUFFER_SIZE = 8192 * 2;
    @Getter
    private DataOutputStream dout;
    @Getter
    private DataInputStream din;

    public TcpConn(@NotNull Socket socket) {
        this.socket = socket;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()), BUFFER_SIZE);
            br = new BufferedReader(new InputStreamReader(socket.getInputStream()), BUFFER_SIZE);
            dout = new DataOutputStream(socket.getOutputStream());
            din = new DataInputStream(socket.getInputStream());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public TcpConn(@NotNull String ip, int port) throws IOException {
        this(new Socket(ip, port));
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
