package org.bitkernel;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;

@Slf4j
public class Udp {
    private static final int BUFF_LEN = 4096;
    private int port;
    private DatagramSocket socket;

    public Udp() {
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            System.exit(-1);
        }
    }

    public Udp(int port) {
        try {
            this.port = port;
            socket = new DatagramSocket(port);
        } catch (SocketException e) {
            logger.error("Attempt to bind udp port {} failed", port);
            System.exit(-1);
        }
    }

    public static boolean checkPort(int port) {
        try  (DatagramSocket socket = new DatagramSocket(port)){
            logger.debug("Udp port {} is available", port);
            return true;
        } catch (Exception e) {
            logger.debug("Udp port {} is unavailable", port);
            return false;
        }
    }

    public void send(String ip, int port,
                     String dataStr) {
        byte[] bytes = dataStr.getBytes();
        InetSocketAddress socAddr = new InetSocketAddress(ip, port);
        DatagramPacket packet = new DatagramPacket(bytes, 0, bytes.length, socAddr);
        try {
            for (int i = 0; i < 3; i++) {
                socket.send(packet);
            }
            logger.debug("UDP send data [{}] to {} success", dataStr, socAddr);
        } catch (IOException e) {
            logger.debug("UDP send data [{}] to {} failed", dataStr, socAddr);
        }
    }

    public DatagramPacket receivePkt() {
        try {
            byte[] buff = new byte[BUFF_LEN];
            DatagramPacket packet = new DatagramPacket(buff, buff.length);
            socket.receive(packet);
            return packet;
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    
    public String receiveString() {
        DatagramPacket packet = receivePkt();
        if (packet == null) {
            return "";
        }
        return pktToString(packet);
    }

    
    public String pktToString( DatagramPacket pkt) {
        byte[] bytes = pkt.getData();
        return new String(bytes, 0, pkt.getLength());
    }

    public void close() {
        socket.close();
    }
}
