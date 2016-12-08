import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Ahmed on 12/6/2016.
 */
public class QueryMaster
{
    class MyHeartBeatThread implements Runnable
    {
        QueryMaster qMaster;

        public MyHeartBeatThread(QueryMaster _qMaster) {
            // store parameter for later user
            qMaster = _qMaster;
        }

        public void run()
        {
            try
            {
                while (!endListiningToHeartBeat)
                {
                    qMaster.checkWorkersStateAndDoWorkShifiting("",qMaster.heartBeatTracker,qMaster.taskDone,qMaster.repliesReceived);
                    Thread.sleep(heartBeatThreshold * 1000);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

    }

    private long heartBeatThreshold = 5;
    private String indexFolder = "G:\\Work\\Java\\Work Space_4\\Index";
    private String[] serversInfo;
    private int myPort = 0;
    private DatagramSocket serverSocket;
    private String queryToRun = "";
    private HashMap<String,Integer> docsRank = new HashMap<String,Integer>();
    private boolean endListiningToHeartBeat = false;


    HashMap<String, Boolean> taskDone = new HashMap<String, Boolean>();
    HashMap<String, LocalDateTime> heartBeatTracker = new HashMap<String, LocalDateTime>();
    HashMap<String, HashMap<String, Boolean>> repliesReceived = new HashMap<String, HashMap<String, Boolean>>();


    public QueryMaster(String Query)
    {
        queryToRun = Query;
    }

    public boolean RunQueryService()
    {
        boolean finishedSuccefully = true;
        try
        {
            ServerSocket s;
            s = new ServerSocket(0);
            myPort = s.getLocalPort();
            s.close();

            String FullQuery = queryToRun;//"I hate hate hate hate people people";
            LinkedList<String> Replies = new LinkedList<String>();
            docsRank = new HashMap<String,Integer>();
            //String FilePath = "G:\\Work\\Java\\Work Space_4\\test.txt";

            //-------------------Name Server
            String nameServerInfo = readNameServerCredentialsFromFile("G:\\Work\\Java\\Work Space_4\\ns.txt");

            String[] parts = nameServerInfo.split("\n");
            int nameServerPort = new Integer(parts[0].trim());
            String nameServerIP = parts[1].trim();

            //--------------------- Asking for workers
            String result = requestWorkers(2, nameServerIP, String.valueOf(nameServerPort));
            result = result.replace("\n", "").replace("\r", "").trim();
            serversInfo = result.split(";");

            //-------------------Task control



            for (int i = 0; i < serversInfo.length; i++)//String server :serversInfo)
            {
                taskDone.put(String.valueOf(i), false);
                heartBeatTracker.put(String.valueOf(i), LocalDateTime.now());
            }
            HashMap<Integer, StringBuilder> queryDistribution = DistributeWordsOnWorkers(FullQuery, serversInfo.length);

            serverSocket = new DatagramSocket(myPort);

            for (int i = 0; i < serversInfo.length; i++)
            {
                int workerID = i;
                String indexPath = indexFolder;// + i + ".txt";

                String workerIP = serversInfo[i].split("_")[0];
                String workerPort = serversInfo[i].split("_")[1];
                String workerQuery = queryDistribution.get(i).toString().trim();
                InitiateTask(i, workerQuery, indexPath, workerIP, workerPort, serversInfo.length);

                HashMap<String, Boolean> wordsForWorker = new HashMap<String, Boolean>();
                for(String word : workerQuery.split(" "))
                {
                    wordsForWorker.put(word.trim(), false);
                }
                repliesReceived.put(String.valueOf(i), wordsForWorker);
            }
            Boolean finished = false;

            StringBuilder allInformation = new StringBuilder();


            Runnable r = new MyHeartBeatThread(this);
            Thread heartBeatThread = new Thread(r);
            heartBeatThread.start();


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
                    String workerID = sentence.split(",")[0];
                    System.out.println("Worker : " + workerID + " Finished");
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
                else
                {
                    Replies.add(sentence.trim());
                    allInformation.append(sentence + "\n");
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

            endListiningToHeartBeat = true;
            docsRank = rerankResults(Replies);
            System.out.println(allInformation.toString());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return finishedSuccefully;
    }

    private  String requestWorkers(int numberOfWorkers, String nsIP, String nsPort)
    {
        try
        {
            Socket requestServerInfoSocket = new Socket(nsIP, Integer.valueOf(nsPort));
            PrintWriter out = new PrintWriter(requestServerInfoSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(requestServerInfoSocket.getInputStream()));
            out.write("G,MR," + numberOfWorkers + "\n");
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

    private  String readNameServerCredentialsFromFile(String _filePath)
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

    private  void InitiateTask(int workerID, String query, String indexFolder,
                                    String workerIP, String workerPort, int numOfReducers)
    {
        String message = "DQW," + workerID + "," + indexFolder + "," + numOfReducers + "," + query;
        try
        {
            //DatagramSocket clientSocket = new DatagramSocket(portNumber);
            byte[] sendData = new byte[2048];

            //String message = word + ":" + currentWordCount;
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(workerIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(workerPort));


            serverSocket.send(sendPacket);
            //clientSocket.close();
        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }


    private  void terminateTask(String workerIP, String workerPort)
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

    private  HashMap<Integer, StringBuilder> DistributeWordsOnWorkers(String Query, int numberOfWorkers)
    {
        String cleanedQuery = Query.replace("\r", "").replace("\n", "").trim().toLowerCase();
        cleanedQuery = cleanedQuery.replace("([^0-9A-Za-z\\s])+", " ");
        cleanedQuery = cleanedQuery.replace("\\s+", " ");
        cleanedQuery = cleanedQuery.trim();

        String[] Tokens = cleanedQuery.split(" ");
        HashSet<String> uniqueWords = new HashSet<String>();
        for (String token : Tokens)
        {
            if (!uniqueWords.contains(token))
            {
                uniqueWords.add(token);
            }
        }
        Tokens = new String[uniqueWords.size()];
        Tokens = uniqueWords.toArray(Tokens);
        HashMap<Integer, StringBuilder> workersWords = new HashMap<Integer, StringBuilder>();
        for (String token : Tokens)
        {
            int ID = hashFunction(token, numberOfWorkers);
            if (workersWords.containsKey(ID))
            {
                StringBuilder sB = workersWords.get(ID);
                sB.append(token + " ");
                workersWords.replace(ID, sB);
            }
            else
            {
                StringBuilder sB = new StringBuilder();
                sB.append(token + " ");
                workersWords.put(ID, sB);
            }
        }

        return workersWords;
    }

    private  int hashFunction(String token, int seed) // seed is the number of reducers -> number of workers as workers are both reducers and mappers
    {
        char firstChar = token.charAt(0);
        int charValue = firstChar - 'a';
        int allChars = 'z' - 'a' + 1;
        int winodwLength = (int) Math.ceil((float) allChars / (float) seed);
        int reducerId = charValue / winodwLength;

        return reducerId;
    }

    private  void HeartBeatHandler(String message, HashMap<String, LocalDateTime> hBeats)
    {
        String[] parts = message.split(",");
        String workerID = parts[1].trim();
        hBeats.replace(workerID, LocalDateTime.now());
    }

    private  LinkedList<String> getNonResponsiveWorkers(HashMap<String, LocalDateTime> hBeats)
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

    private  void checkWorkersStateAndDoWorkShifiting(String heartBeatMessage, HashMap<String, LocalDateTime> heartBeatTracker, HashMap<String, Boolean> taskState, HashMap<String, HashMap<String, Boolean>> repliesReceived)
    {
        if(taskState.keySet().size() == 0)
        {
            try
            {
                DatagramSocket dS = new DatagramSocket();
                byte[] sendData = new byte[2048];

                String message = "TERMINATE";
                sendData = message.getBytes();
                String myIpAddress = getMyIP();
                InetAddress IPAddress = InetAddress.getByName(myIpAddress);
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, myPort);
                dS.send(sendPacket);
                dS.close();
            } catch (Exception e)
            {
            }
        }
        if(!heartBeatMessage.trim().equals(""))
        {
            HeartBeatHandler(heartBeatMessage, heartBeatTracker);
        }
        LinkedList<String> nonResponsiveIDs = getNonResponsiveWorkers(heartBeatTracker);
        for (String fallenWorkerID : nonResponsiveIDs)
        {
            if (taskState.get(fallenWorkerID) == true)
            {
            }
            else
            {
                HashMap<String, Boolean> wordsSupposedToResponedOn = repliesReceived.get(fallenWorkerID);
                String wordsNotRespondedOnFromWorker = "";
                for (String word : wordsSupposedToResponedOn.keySet())
                {
                    if (wordsSupposedToResponedOn.get(word) == false)
                    {
                        wordsNotRespondedOnFromWorker += word + " ";
                    }
                }
                wordsNotRespondedOnFromWorker = wordsNotRespondedOnFromWorker.trim();
                if (wordsNotRespondedOnFromWorker.equals(""))
                {
                    // no need to find another worker, this worker word has been done and can be marked as finished
                    taskState.replace(fallenWorkerID, true);
                }
                else
                {
                    // we should assign the remaining work to another worker
                    String desiredWorkerID = "";
                    for (String worker : taskState.keySet())
                    {
                        if (taskState.get(worker) == true)
                        {
                            desiredWorkerID = worker;
                            break;
                        }
                    }
                    if (!desiredWorkerID.equals(""))
                    {
                        taskState.remove(fallenWorkerID);
                        taskState.replace(desiredWorkerID, false);
                        repliesReceived.remove(fallenWorkerID);
                        repliesReceived.remove(desiredWorkerID);
                        HashMap<String, Boolean> wordsToSearchAgain = new HashMap<String, Boolean>();
                        String[] words = wordsNotRespondedOnFromWorker.split(" ");
                        for (String _word : words)
                        {
                            wordsToSearchAgain.put(_word, false);
                        }
                        repliesReceived.put(desiredWorkerID, wordsToSearchAgain);

                        String workerIP = serversInfo[Integer.valueOf(desiredWorkerID)].split("_")[0];
                        String workerPort = serversInfo[Integer.valueOf(desiredWorkerID)].split("_")[1];
                        InitiateTask(Integer.valueOf(desiredWorkerID), wordsNotRespondedOnFromWorker, indexFolder, workerIP, workerPort, serversInfo.length);
                    }

                }
            }
        }
    }

    private HashMap<String,Integer> rerankResults(LinkedList<String> replies)
    {
        HashMap<String, Integer> _docsRank = new HashMap<String, Integer>();

        for(String reply : replies)
        {
            String[] Parts = reply.trim().split(":");
            if(Parts.length > 2)
            {
                if(!Parts[2].equals(""))
                {
                    String docsInfo = Parts[2];
                    String[] docs = docsInfo.split(";");
                    for(String docInfo : docs)
                    {
                        String docId = docInfo.trim().split(",")[0];
                        int wordCount = Integer.valueOf(docInfo.trim().split(",")[1]);
                        if(_docsRank.containsKey(docId))
                        {
                            int oldRank = _docsRank.get(docId);
                            oldRank+= 100 + wordCount;
                            _docsRank.replace(docId,oldRank);
                        }
                        else
                        {
                            _docsRank.put(docId,100 + wordCount);
                        }
                    }
                }
            }

        }
        return _docsRank;
    }

    public HashMap<String,Integer> getQueryResult()
    {
        return docsRank.entrySet()
                .stream()
                .sorted(HashMap.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
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
}
