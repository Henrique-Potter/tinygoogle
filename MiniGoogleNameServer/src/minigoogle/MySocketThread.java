package minigoogle;

import minigoogle.domain.WorkStatusComparator;
import minigoogle.domain.WorkerStatus;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class MySocketThread implements Runnable {

    private Socket cs;
    private HashMap<String, WorkerStatus> workersDictionary;
    private int maxWorkerNumber;
    private MiniGNameServerKernel serverKernel;

    public MySocketThread(Socket _cs, HashMap wDic,MiniGNameServerKernel sKernel) {
        cs = _cs;
        workersDictionary = wDic;
        serverKernel = sKernel;
    }

    public void run() {
        decodeRequest();
    }

    public void decodeRequest() {
        PrintWriter out = null;
        BufferedReader in = null;
        try {
            out = new PrintWriter(cs.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(cs.getInputStream()));

            String wholeText = "";
            wholeText += in.readLine();

            if (!wholeText.equals(null)) {
                System.out.println(wholeText);
            }

            wholeText = wholeText.replace("\n", "").replace("\r", "").trim();

            System.out.println("Receiving connection -> "+"Request: "+wholeText + new java.util.Date());
            switch (wholeText.charAt(0)) {
                case 'R': {
                    registerClient(out, wholeText);

                    break;
                }
                case 'G': {
                    getAvailableWorkers(out);
                    break;
                }
                case 'W': {
                    setAliveWorker(wholeText);
                    break;
                }
                case 'T': {
                    updateWorkerUnderUseBeat(wholeText);
                    break;
                }
                case 'S': {
                    releaseWorkers(wholeText);
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        } finally {
            serverKernel.serverSaveState();
            close(cs);
            close(out);
            close(in);
        }
    }

    private void releaseWorkers(String text){
        String parts[] = text.split(",");
        String freeID = parts[1].trim();

        Iterator it = workersDictionary.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry c = (Map.Entry) it.next();
            WorkerStatus w = (WorkerStatus)c.getValue();

            if(w.getServerOwner().equals(text)){
                w.setWorkerCurrentStatus("I");
            }
        }

    }

    private void setAliveWorker(String text) {
        String parts[] = text.split(",");
        String Ip = parts[1].trim();
        String Port = parts[2].trim();

        String workerID = Ip + "_" + Port;

        if(workersDictionary.containsKey(workerID)){
            WorkerStatus w = workersDictionary.get(workerID);
            w.setLastAliveBeat();
        }
    }

    public  void updateWorkerUnderUseBeat(String text)
    {
        String[] strParts = text.split(",");
        String serverID = strParts[1];

        List<WorkerStatus> workers = new ArrayList<>();

        Iterator it = workersDictionary.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry c = (Map.Entry) it.next();
            WorkerStatus w = (WorkerStatus)c.getValue();

            if(w.getWorkerCurrentStatus().equals("B")&&w.getServerOwner().equals(serverID)){
                w.getServerUseLiveBeat();
            }
        }
        Collections.sort(workers,new WorkStatusComparator());

    }

    private void getAvailableWorkers(PrintWriter out) {
        List<WorkerStatus> availableServers = getListOfWorkers();
        if (availableServers.size() == 0) {
            System.out.println("No workers found");
            out.write("NF\n");
            out.flush();
        } else {
            StringBuilder sB = new StringBuilder();
            sB.append(availableServers.get(0).getServerOwner());
            sB.append(";");
            for (WorkerStatus workerStatus : availableServers) {
                sB.append(workerStatus.getIpAddress()+"_"+workerStatus.getPortNumber());
                sB.append(";");
            }
            out.write(sB.toString() + "\n");
            out.flush();
        }
    }

    private void registerClient(PrintWriter out, String wholeText) {
        boolean regResult = registerWorker(wholeText);
        if (regResult == true) {
            out.write("D\n");
            out.flush();
        } else {
            out.write("F\n");
            out.flush();
        }
    }

    private void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public  List<WorkerStatus> getListOfWorkers() // if number of workers == 0, return the all idle workers
    {
        List<String> workersNames = new ArrayList<>();
        List<WorkerStatus> workers = new ArrayList<>();

        Iterator it = workersDictionary.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry c = (Map.Entry) it.next();
            WorkerStatus w = (WorkerStatus)c.getValue();

            if(w.getWorkerCurrentStatus().equals("I")){
                workersNames.add((String)c.getKey());
                UUID uuid = UUID.randomUUID();
                w.setWorkerCurrentStatus("B");
                w.setServerOwner(uuid.toString());
                workers.add((WorkerStatus) c.getValue());
            }
        }
        Collections.sort(workers,new WorkStatusComparator());
        return workers;
    }

    public boolean registerWorker(String text) {
        try {
            String parts[] = text.split(",");

            String workerType = parts[1].trim();
            String Ip = parts[2].trim();
            String Port = parts[3].trim();

            String workerID = Ip + "_" + Port;
            WorkerStatus ws = new WorkerStatus();

            ws.setIpAddress(Ip);
            ws.setPortNumber(Port);
            ws.setWorkerType(workerType);
            ws.setWorkerCurrentStatus("I");

            if (!workersDictionary.containsKey(workerID)) {
                ws.setLastAliveBeat();
                ws.setServerUseLiveBeat();
                workersDictionary.put(workerID, ws);
            }
            if (workerType.equals("M")) {
                System.out.println("Worker with IP: " + Ip + ", and Port number " + Port + " is registered as Mapper");
            } else if (workerType.equals("R")) {
                System.out.println("Worker with IP: " + Ip + ", and Port number " + Port + " is registered as Reducer");
            } else if (workerType.equals("MR") || workerType.equals("RM")) {
                System.out.println("Worker with IP: " + Ip + ", and Port number " + Port + " is registered as Mapper and Producer");
            }
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

}
