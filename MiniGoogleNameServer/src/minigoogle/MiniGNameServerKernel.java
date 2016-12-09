package minigoogle;

import com.fasterxml.jackson.databind.ObjectMapper;
import minigoogle.domain.NameServerSavedState;
import minigoogle.domain.WorkerStatus;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class MiniGNameServerKernel {

    public static String filePath = "..//ns.txt";
    public static String backupFilePath = "..//backup.json";

    public static HashMap<String, WorkerStatus> workersDictionary = new HashMap<String, WorkerStatus>();

    private ServerSocket serverSocket;

    private ScheduledExecutorService scheduledExecutor = newSingleThreadScheduledExecutor();

    private ExecutorService executor;

    private final long workerBeatDeadLine = 10;
    private final long workerUsageDeadLine = 20;

    private final ObjectMapper mapper = new ObjectMapper();

    private NameServerSavedState serverState = new NameServerSavedState();

    public MiniGNameServerKernel() {
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void initializeServer() {

        startResilienceThread();
        try {
            serverSocket = new ServerSocket(58740);
            int port = serverSocket.getLocalPort();
            String ip = serverSocket.getInetAddress().getLocalHost().getHostAddress();
            System.out.println(ip +" "+ port);
            while (true) {
                try {
                    Socket clientSocket;
                    clientSocket = serverSocket.accept();

                    executor.execute(new MySocketThread(clientSocket, workersDictionary, this));
                } catch (Exception ex) {
                    System.err.println(ex);

                } finally {
                    serverSaveState();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startResilienceThread() {

        scheduledExecutor.scheduleAtFixedRate(() -> {


            eraseDeadWorkers();
            freeWorkersUnderUse();

            System.out.println("Cleanning done: " + new java.util.Date());
        }, 10, 10, TimeUnit.SECONDS);

    }

    private void eraseDeadWorkers() {
        Iterator it = workersDictionary.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry c = (Map.Entry) it.next();

            WorkerStatus w = (WorkerStatus) c.getValue();

            Instant workerLastBeat = w.getLastAliveBeat();
            long timeElapsed = workerLastBeat.until(Instant.now(), ChronoUnit.SECONDS);

            if (workerBeatDeadLine < timeElapsed) {
                it.remove();
                System.out.println("Worker with IP: " + w.getIpAddress() + " and port number " + w.getPortNumber() + " have been removed.");
            }
        }
    }

    private void freeWorkersUnderUse() {
        Iterator it = workersDictionary.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry c = (Map.Entry) it.next();

            WorkerStatus w = (WorkerStatus) c.getValue();

            Instant workerLastUsageBeat = w.getServerUseLiveBeat();
            long timeElapsed = workerLastUsageBeat.until(Instant.now(), ChronoUnit.SECONDS);

            if (workerUsageDeadLine < timeElapsed) {
                w.setWorkerCurrentStatus("I");
                System.out.println("Worker with IP: " + w.getIpAddress() + " and port number " + w.getPortNumber() + " is now Idle.");
            }
        }
    }

    private void evaluateInUseWorkers() {
        Iterator it = workersDictionary.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry c = (Map.Entry) it.next();

            WorkerStatus w = (WorkerStatus) c.getValue();

            Instant workerLastBeat = w.getLastAliveBeat();
            long timeElapsed = workerLastBeat.until(Instant.now(), ChronoUnit.SECONDS);

            if (workerBeatDeadLine < timeElapsed) {
                it.remove();
                System.out.println("Worker with IP: " + w.getIpAddress() + " and port number " + w.getPortNumber() + " have been removed.");
            }
        }
    }

    public void serverSaveState() {
        serverState.workersDictionary = this.workersDictionary;
        try {
            mapper.writeValue(new File(backupFilePath), serverState);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void serverRestoreState() {
        try {
            serverState = mapper.readValue(new FileReader(backupFilePath), NameServerSavedState.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
