import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by Ahmed on 12/3/2016.
 */
public class IndexMaster
{
    private class MyHeartBeatThread implements Runnable
    {
        IndexMaster iMaster;

        public MyHeartBeatThread(IndexMaster _iMaster)
        {
            // store parameter for later user
            iMaster = _iMaster;
        }

        public void run()
        {
            try
            {
                while (!endListiningToHeartBeat)
                {
                    checkWorkersStateAndDoWorkShifiting("", heartBeatTracker, taskDone);
                    Thread.sleep(heartBeatThreshold * 1000);
                }
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }

    }

    private class failedPart
    {
        int startIndex = 0;
        int Length = 0;
    }

    private String FilePath = "G:\\Work\\Java\\Work Space_4\\test.txt";
    private int numberOfWorkers = 2;
    private boolean endListiningToHeartBeat = false;
    private int heartBeatThreshold = 5;
    private HashMap<String, Boolean> taskDone = new HashMap<String, Boolean>();
    private HashMap<String, LocalDateTime> heartBeatTracker = new HashMap<String, LocalDateTime>();
    private String[] serversInfo;

    private String indexFolder = "";
    private int DocID = 0;
    private int _myPortNumber = 0;
    private int failedWorkerID = -1;
    private String requestID = "";

    public IndexMaster(String filePath, int _DocID, String indexFolderPath)
    {
        FilePath = filePath;
        //numberOfWorkers = _numberOfWorkers;
        taskDone = new HashMap<String, Boolean>();
        DocID = _DocID;
        indexFolder = indexFolderPath;
    }

    public boolean RunIndexService()
    {
        boolean finishState = true;
        failedWorkerID = -1;
        try
        {
            ServerSocket s;
            s = new ServerSocket(0);
            int myPort = s.getLocalPort();
            _myPortNumber = myPort;
            s.close();


            String nameServerInfo = readNameServerCredentialsFromFile("G:\\Work\\Java\\Work Space_4\\ns.txt");
            String[] parts = nameServerInfo.split("\n");
            int nameServerPort = new Integer(parts[0].trim());
            String nameServerIP = parts[1].trim();

            String result = requestWorkers(nameServerIP, String.valueOf(nameServerPort));
            result = result.replace("\n", "").replace("\r", "").trim();

            //-----------------------------------------------------------
            if(result.equals("NF"))
            {
                return false;
            }
            String[] allParts = result.split(";");
            requestID = allParts[0].trim();
            serversInfo = new String[allParts.length - 1];
            for(int i =1; i < allParts.length;i++)
            {
                serversInfo[i-1] = allParts[i];
            }
            //serversInfo = result.split(";");
            numberOfWorkers = serversInfo.length;

            finishState =  indexDocument();
            if(finishState == false)
            {
                if (failedWorkerID > -1 && serversInfo.length > 1)
                {
                    String[] newServerInfo = new String[serversInfo.length - 1];
                    int counter = 0;
                    for (String st : serversInfo)
                    {
                        if(counter == failedWorkerID)
                        {
                            newServerInfo[counter] = st;
                            counter++;
                        }
                    }
                    serversInfo = newServerInfo;
                    finishState = indexDocument();
                }
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return finishState;
    }

    private boolean indexDocument()
    {
        boolean finishedSuccefully = true;
        taskDone = new HashMap<String, Boolean>();
        try
        {
            for (int i = 0; i < serversInfo.length; i++)//String server :serversInfo)
            {
                taskDone.put(String.valueOf(i), false);
                heartBeatTracker.put(String.valueOf(i), LocalDateTime.now());
            }
            String workerPhoneBook = "";
            for (int i = 0; i < serversInfo.length; i++)
            {
                workerPhoneBook += i + " " + serversInfo[i] + ";";
            }
            workerPhoneBook = workerPhoneBook.substring(0, workerPhoneBook.length() - 1);
            int textPieceLength = readDocumentAndgetNumberPartLength(FilePath,serversInfo.length);
            for (int i = 0; i < serversInfo.length; i++)
            {
                int workerID = i;
                String indexPath = indexFolder;// + i + ".txt";

                String workerIP = serversInfo[i].split("_")[0];
                String workerPort = serversInfo[i].split("_")[1];

                InitiateTask(_myPortNumber, i, String.valueOf(DocID), FilePath, String.valueOf(i * textPieceLength), String.valueOf(textPieceLength), indexPath, workerIP, workerPort, serversInfo.length, workerPhoneBook);
            }
            Boolean finished = false;

            Runnable r = new MyHeartBeatThread(this);
            Thread heartBeatThread = new Thread(r);
            heartBeatThread.start();

            DatagramSocket serverSocket = new DatagramSocket(_myPortNumber);
            while (!finished)
            {
                System.out.println("Looping waiting for finishing signals");

                byte[] receiveData = new byte[2048];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                String sentence = new String(receivePacket.getData());
                sentence = sentence.replace("\n", "").replace("\r", "").trim();
                if (sentence.contains(",Done"))
                {
                    System.out.println("RECEIVED: " + sentence);
                    InetAddress IPAddress = receivePacket.getAddress();
                    int port = receivePacket.getPort();
                    String finishedWorkerIP = IPAddress.toString();
                    finishedWorkerIP = finishedWorkerIP.replace("/", "");
                    String key = finishedWorkerIP + "_" + port;
                    System.out.println(key + " Finished");

                    String workerID = sentence.split(",")[0];
                    taskDone.replace(workerID, true);
                }

                else if (sentence.substring(0, 4).equals("HBM,"))
                {
                    HeartBeatHandler(sentence, heartBeatTracker);
                }
                else if (sentence.equals("TERMINATE"))
                {
                    // Something wrong i sent to my self
                    finishedSuccefully = false;
                    break;
                }
                finished = true;
                for (String key : taskDone.keySet())
                {
                    if (taskDone.get(key) == false)
                    {
                        finished = false;
                        break;
                    }
                }
            }

            for (String key : serversInfo)
            {
                System.out.println("Terminating workers");
                String IP = key.split("_")[0];
                String Port = key.split("_")[1];
                terminateTask(IP, Port);
            }

            serverSocket.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

        endListiningToHeartBeat = true;

        return finishedSuccefully;
    }

    private void indexPart(String path, int startingIndex, int length, String _workerID, String workerInfo)
    {
        try
        {
            ServerSocket s;
            s = new ServerSocket(0);
            int myPort = s.getLocalPort();
            s.close();

            String[] serversInfo = new String[]{workerInfo};
            taskDone.put(_workerID, false);
            String workerPhoneBook = _workerID + " " + workerInfo;
            String indexPath = indexFolder;// + i + ".txt";

            String workerIP = workerInfo.split("_")[0];
            String workerPort = workerInfo.split("_")[1];
            InitiateTask(myPort, Integer.valueOf(_workerID), String.valueOf(DocID), FilePath, String.valueOf(startingIndex), String.valueOf(length), indexPath, workerIP, workerPort, serversInfo.length, workerPhoneBook);

            Boolean finished = false;
            DatagramSocket serverSocket = new DatagramSocket(myPort);
            while (!finished)
            {
                System.out.println("Looping waiting for finishing signals");

                byte[] receiveData = new byte[2048];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                String sentence = new String(receivePacket.getData());
                sentence = sentence.replace("\n", "").replace("\r", "").trim();
                if (sentence.contains(",Done"))
                {
                    System.out.println("RECEIVED: " + sentence);
                    InetAddress IPAddress = receivePacket.getAddress();
                    int port = receivePacket.getPort();
                    String finishedWorkerIP = IPAddress.toString();
                    finishedWorkerIP = finishedWorkerIP.replace("/", "");
                    String key = finishedWorkerIP + "_" + port;
                    System.out.println(key + " Finished");

                    String workerID = sentence.split(",")[0];
                    taskDone.replace(workerID, true);
                }
                else if (sentence.substring(0, 4).equals("HBM,"))
                {
                    HeartBeatHandler(sentence, heartBeatTracker);
                }

                finished = true;
                for (String key : taskDone.keySet())
                {
                    if (taskDone.get(key) == false)
                    {
                        finished = false;
                        break;
                    }
                }
            }

            for (String key : serversInfo)
            {
                System.out.println("Terminating workers");
                String IP = key.split("_")[0];
                String Port = key.split("_")[1];
                terminateTask(IP, Port);
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private String requestWorkers(String nsIP, String nsPort)
    {
        try
        {
            Socket requestServerInfoSocket = new Socket(nsIP, Integer.valueOf(nsPort));
            PrintWriter out = new PrintWriter(requestServerInfoSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(requestServerInfoSocket.getInputStream()));
            out.write("G,MR" + "\n");
            out.flush();
            //out.println("req:"+serviceName);


            String inputLine;
            String wholeText = "";
            //in.wait();
            wholeText = in.readLine();
		    /*while ((inputLine = in.readLine()) != null)
	    	{
	    		wholeText+=inputLine + "\n";
	    	}*/
            requestServerInfoSocket.close();
            return wholeText.trim();
        } catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return "";
    }

    private String readNameServerCredentialsFromFile(String _filePath)
    {
        String FileContent = "";
        try
        {
            File tempFile = new File(_filePath);
            if (tempFile.exists())
            {

            }
            else
            {
                _filePath = "..//" + _filePath;
            }

            FileReader inputFile = new FileReader(_filePath);
            BufferedReader bufferReader = new BufferedReader(inputFile);
            String line;

            // Read file line by line and print on the console
            while ((line = bufferReader.readLine()) != null)
            {
                FileContent += line + "\n";
            }
            bufferReader.close();
        } catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return FileContent;

    }

    private void InitiateTask(int portNumber, int workerID, String docID, String filePath,
                              String startingIndex, String chunkLength, String indexPath,
                              String workerIP, String workerPort, int numOfReducers, String phoneBook)
    {
        String message = "DIW," + workerID + "," + docID + "," + filePath + "," + startingIndex + "," + chunkLength + "," + indexPath + "," + numOfReducers + "," + phoneBook;
        try
        {
            DatagramSocket clientSocket = new DatagramSocket(portNumber);
            byte[] sendData = new byte[2048];

            //String message = word + ":" + currentWordCount;
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(workerIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(workerPort));


            clientSocket.send(sendPacket);
            clientSocket.close();
        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }


    private void terminateTask(String workerIP, String workerPort)
    {
        try
        {
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData = new byte[2048];

            String message = "endprocess";
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(workerIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(workerPort));
            clientSocket.send(sendPacket);
            clientSocket.close();
        } catch (Exception e)
        {

        }
    }


    private void checkWorkersStateAndDoWorkShifiting(String heartBeatMessage, HashMap<String, LocalDateTime> heartBeatTracker, HashMap<String, Boolean> taskState)
    {
        if (!heartBeatMessage.trim().equals(""))
        {
            HeartBeatHandler(heartBeatMessage, heartBeatTracker);
        }
        boolean foundFalse = false;
        /*for(String state : taskState.keySet())
        {
            if(taskState.get(state) == false)
            {
                foundFalse = true;
            }
        }
        if(!foundFalse)
        {
            try
            {
                DatagramSocket dS = new DatagramSocket();
                byte[] sendData = new byte[2048];

                String message = "TERMINATE";
                sendData = message.getBytes();
                String myIpAddress = getMyIP();
                InetAddress IPAddress = InetAddress.getByName(myIpAddress);
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, _myPortNumber);
                dS.send(sendPacket);
                dS.close();
            } catch (Exception e)
            {
            }
        }*/
        LinkedList<String> nonResponsiveIDs = getNonResponsiveWorkers(heartBeatTracker);
        for (String fallenWorkerID : nonResponsiveIDs)
        {
            if (taskState.get(fallenWorkerID) == true)
            {
            }
            else
            {
                failedWorkerID = Integer.valueOf(fallenWorkerID);
                if (serversInfo != null)
                {
                    for (String key : serversInfo)
                    {
                        System.out.println("Terminating workers");
                        String IP = key.split("_")[0];
                        String Port = key.split("_")[1];
                        terminateTask(IP, Port);
                    }
                    try
                    {
                        DatagramSocket dS = new DatagramSocket();
                        byte[] sendData = new byte[2048];

                        String message = "TERMINATE";
                        sendData = message.getBytes();
                        String myIpAddress = getMyIP();
                        InetAddress IPAddress = InetAddress.getByName(myIpAddress);
                        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, _myPortNumber);
                        dS.send(sendPacket);
                        dS.close();
                    } catch (Exception e)
                    {
                    }
                }

            }
        }
    }

    private LinkedList<String> getNonResponsiveWorkers(HashMap<String, LocalDateTime> hBeats)
    {
        LinkedList<String> nonResponsiveWorkers = new LinkedList<String>();
        for (String id : hBeats.keySet())
        {
            long seconds = ChronoUnit.SECONDS.between(hBeats.get(id), LocalDateTime.now());
            if (seconds > heartBeatThreshold)
            {
                nonResponsiveWorkers.add(id);
            }
            //long minutes = ChronoUnit.MINUTES.between(hBeats.get(id), LocalDateTime.now());
            //long hours = ChronoUnit.HOURS.between(fromDate, toDate);
        }
        return nonResponsiveWorkers;
    }

    private void HeartBeatHandler(String message, HashMap<String, LocalDateTime> hBeats)
    {
        String[] parts = message.split(",");
        String workerID = parts[1].trim();
        hBeats.replace(workerID, LocalDateTime.now());
    }

    private String getMyIP()
    {

        String ip = "";
        try
        {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements())
            {
                NetworkInterface iface = interfaces.nextElement();
                // filters out 127.0.0.1 and inactive interfaces
                if (iface.isLoopback() || !iface.isUp())
                    continue;

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements())
                {
                    InetAddress addr = addresses.nextElement();
                    ip = addr.getHostAddress();
                    return ip;
                    //if(ip.contains("192.168.1"))
                    //	{
                    //		return ip;
                    //	}
                    //System.out.println(iface.getDisplayName() + " " + ip);
                }
            }
        } catch (SocketException e)
        {
            //throw new RuntimeException(e);
            //return "192.168.1.1";
        }
        return "192.168.1.1";
    }

    private int readDocumentAndgetNumberPartLength(String filePath, int numberOfReducersUsed)
    {
        StringBuilder Content = new StringBuilder();
        String _filePath = filePath;
        try
        {
            File tempFile = new File(_filePath);
            if (tempFile.exists())
            {
                FileReader inputFile = new FileReader(_filePath);
                BufferedReader bufferReader = new BufferedReader(inputFile);

                int count = 0;
                int intch;
                while (((intch = bufferReader.read()) != -1))
                {
                    count++;
                }

                bufferReader.close();
                int pieceLength = (int)(count / numberOfReducersUsed) + 1;
                return pieceLength;
            }
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;
    }

    public void releaseWorkers()
    {
        String nameServerInfo = readNameServerCredentialsFromFile("G:\\Work\\Java\\Work Space_4\\ns.txt");
        String[] parts = nameServerInfo.split("\n");
        int nameServerPort = new Integer(parts[0].trim());
        String nameServerIP = parts[1].trim();

        try
        {
            Socket requestServerInfoSocket = new Socket(nameServerIP, Integer.valueOf(nameServerPort));
            PrintWriter out = new PrintWriter(requestServerInfoSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(requestServerInfoSocket.getInputStream()));
            out.write("S," +requestID + "\n");
            out.flush();
            requestServerInfoSocket.close();
        } catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
