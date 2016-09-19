package com.navercorp.pinpoint.thrift.io;

import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;
import org.apache.thrift.TBase;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Logger;

/**
 * Created by wangyue1 on 2016/9/18.
 */
public class HeaderTBaseTest {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(HeaderTBaseSerializerTest.class.getName());

    @Test
    public void headerThrift() throws Exception {
        TSqlMetaData smd = new TSqlMetaData();
        smd.setAgentId("agentId");
        smd.setAgentStartTime(10);
        smd.setSqlId(1);
        smd.setSql("SQL");

        byte[] bs = HeaderTBaseSerializerFactory.DEFAULT_FACTORY.createSerializer().serialize(smd);

        System.out.println("");
        for (byte b : bs) {
            System.out.print(b);
            System.out.print(",");
        }
        System.out.println("");

        TBase t = HeaderTBaseDeserializerFactory.DEFAULT_FACTORY.createDeserializer().deserialize(bs);

        System.out.println(t.toString());
    }

    @Test
    public void udpTest() throws Exception {
        new Server().enter();
    }

    public class Server {

        private DatagramSocket socket;
        private String bindAddress;
        private int port;

        private ThreadPoolExecutor io;

        public Server() {
            this.socket = createSocket(1024 * 1024 * 1024);
            this.bindAddress = "127.0.0.1";
            this.port = 9877;
            io = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        }

        private DatagramSocket createSocket(int receiveBufferSize) {
            try {
                DatagramSocket socket = new DatagramSocket(null);
                socket.setReceiveBufferSize(receiveBufferSize);

                final int checkReceiveBufferSize = socket.getReceiveBufferSize();
                if (receiveBufferSize != checkReceiveBufferSize) {
                    logger.warn("DatagramSocket.setReceiveBufferSize() error. " + receiveBufferSize + "!=" + checkReceiveBufferSize);
                }

//            socket.setSoTimeout(1000 * 5);
                return socket;
            } catch (SocketException ex) {
                throw new RuntimeException("Socket create Fail. Caused:" + ex.getMessage(), ex);
            }
        }

        private void bindSocket(DatagramSocket socket, String bindAddress, int port) {
            if (socket == null) {
                throw new NullPointerException("socket must not be null");
            }
            try {
                logger.info("DatagramSocket.bind() " + bindAddress + "/" + port);
                socket.bind(new InetSocketAddress(bindAddress, port));
            } catch (SocketException ex) {
                throw new IllegalStateException("Socket bind Fail. port:" + port + " Caused:" + ex.getMessage(), ex);
            }
        }

        public void start() {
            final DatagramSocket socket = this.socket;
            if (socket == null) {
                throw new IllegalStateException("socket is null.");
            }
            bindSocket(socket, bindAddress, port);

            for (int i = 0; i < 10; i++) {
                io.execute(new Runnable() {
                    @Override
                    public void run() {
                        receive(socket);
                    }
                });
            }

        }

        private void receive(DatagramSocket socket) {

            try {
                byte[] receiveData = new byte[1024];
                while (true) {
                    DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                    socket.receive(receivePacket);

                    TBase tSqlMetaData = HeaderTBaseDeserializerFactory.DEFAULT_FACTORY.createDeserializer().deserialize(receivePacket.getData());

                    InetAddress ip = receivePacket.getAddress();
                    int port = receivePacket.getPort();

                    System.out.println("receive " + ip + ":" + port + " <= \n" + tSqlMetaData.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void enter() throws IOException, InterruptedException {
            Server server = new Server();
            server.start();

            Thread.sleep(1000 * 60 * 60 * 24);

            InetSocketAddress address = new InetSocketAddress("0.0.0.0", 9877);
            DatagramSocket serverSocket = new DatagramSocket(address);
            serverSocket.setReceiveBufferSize(1024 * 1024);
            byte[] receiveData = new byte[1024];
            while (true) {
                System.out.println("try to receive");

                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);

                String sentence = new String(receivePacket.getData());
                System.out.println("RECEIVED: " + sentence);

                InetAddress ip = receivePacket.getAddress();
                int port = receivePacket.getPort();

                System.out.println("receive " + ip + ":" + port + " <= " + sentence);
            }
        }
    }
}