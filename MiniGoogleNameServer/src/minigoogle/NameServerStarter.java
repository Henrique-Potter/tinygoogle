package minigoogle;


import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

public class NameServerStarter {

    public static void main(String[] args) {
        MiniGNameServerKernel miniGoogle = new MiniGNameServerKernel();
        miniGoogle.initializeServer();
    }

}
