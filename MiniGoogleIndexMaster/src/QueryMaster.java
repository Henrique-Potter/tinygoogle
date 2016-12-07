import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Created by Ahmed on 12/6/2016.
 */
public class QueryMaster
{
    public static void main(String args[]) throws Exception
    {
        ServerSocket s;
        s = new ServerSocket(0);
        int myPort = s.getLocalPort();
        s.close();

        String FullQuery = "I hate hate hate hate people people";
        String FilePath = "G:\\Work\\Java\\Work Space_4\\test.txt";

        //-------------------Name Server
        String nameServerInfo = readNameServerCredentialsFromFile("G:\\Work\\Java\\Work Space_4\\ns.txt");
        String indexFolder = "G:\\Work\\Java\\Work Space_4\\Index";
        String[] parts = nameServerInfo.split("\n");
        int nameServerPort = new Integer(parts[0].trim());
        String nameServerIP = parts[1].trim();

        //--------------------- Asking for workers
        String result = requestWorkers(2,nameServerIP,String.valueOf(nameServerPort));
        result = result.replace("\n","").replace("\r","").trim();
        String[] serversInfo = result.split(";");

        //-------------------Task control

        HashMap<String, Boolean> taskDone = new HashMap<String, Boolean>();
        for(int i =0;i < serversInfo.length;i++)//String server :serversInfo)
        {
            taskDone.put(String.valueOf(i), false);
        }
        HashMap<Integer,StringBuilder> queryDistribution = DistributeWordsOnWorkers(FullQuery, serversInfo.length);
        //String workerPhoneBook = "";
        //for(int i =0; i < serversInfo.length;i++)
        //{
        //    workerPhoneBook+= i+" " + serversInfo[i] + ";";
       // }
        //workerPhoneBook = workerPhoneBook.substring(0,workerPhoneBook.length() - 1);
        for(int i =0; i < serversInfo.length;i++)
        {
            int workerID = i;
            String indexPath = indexFolder;// + i + ".txt";

            String workerIP = serversInfo[i].split("_")[0];
            String workerPort = serversInfo[i].split("_")[1];
            String workerQuery = queryDistribution.get(i).toString().trim();
            InitiateTask(myPort,i,workerQuery,indexPath,workerIP,workerPort,serversInfo.length);
        }
        Boolean finished = false;

        StringBuilder allInformation = new StringBuilder();
        DatagramSocket serverSocket = new DatagramSocket(myPort);
        while(!finished)
        {
            System.out.println("Looping waiting for finishing signals");

            byte[] receiveData = new byte[64];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            serverSocket.receive(receivePacket);
            String sentence = new String(receivePacket.getData());
            sentence = sentence.replace("\n","").replace("\r","").trim();
            if(sentence.contains(",Done"))
            {
                System.out.println("RECEIVED: " + sentence);
                /*InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();
                String finishedWorkerIP = IPAddress.toString();
                finishedWorkerIP = finishedWorkerIP.replace("/","");
                String key = finishedWorkerIP + "_" + port;
                */
                String workerID = sentence.split(",")[0];
                System.out.println( "Worker : " + workerID + " Finished");
                taskDone.replace(workerID, true);
            }
            else
            {
                allInformation.append(sentence + "\n");
            }
            finished = true;
            for(String key : taskDone.keySet())
            {
                if(taskDone.get(key) == false)
                {
                    finished = false;
                    break;
                }
            }
        }

        System.out.println(allInformation.toString());
        /*for(String key :  serversInfo)
        {
            System.out.println("Terminating workers");
            String IP = key.split("_")[0];
            String Port = key.split("_")[1];
            terminateTask(IP,Port);
        }*/
    }

    public static String requestWorkers(int numberOfWorkers, String nsIP, String nsPort)
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

    public static String readNameServerCredentialsFromFile(String _filePath)
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

    public static void InitiateTask(int portNumber, int workerID, String query, String indexFolder,
                                    String workerIP, String workerPort, int numOfReducers)
    {
        String message = "DQW," + workerID + ","+ indexFolder+","+numOfReducers + "," +query ;
        try
        {
            DatagramSocket clientSocket = new DatagramSocket(portNumber);
            byte[] sendData = new byte[1024];

            //String message = word + ":" + currentWordCount;
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(workerIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(workerPort));


            clientSocket.send(sendPacket);
            clientSocket.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }


    public static void terminateTask(String workerIP, String workerPort)
    {
        try
        {
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData = new byte[64];

            String message = "endprocess";
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(workerIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(workerPort));
            clientSocket.send(sendPacket);
            clientSocket.close();
        }
        catch(Exception e)
        {

        }
    }

    public static HashMap<Integer, StringBuilder> DistributeWordsOnWorkers(String Query, int numberOfWorkers)
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
        Tokens =  uniqueWords.toArray(Tokens);
        HashMap<Integer, StringBuilder> workersWords = new HashMap<Integer, StringBuilder>();
        for (String token : Tokens)
        {
            int ID = hashFunction(token, numberOfWorkers);
            if (workersWords.containsKey(ID))
            {
                StringBuilder sB= workersWords.get(ID);
                sB.append(token + " ");
                workersWords.replace(ID,sB);
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

    public static int hashFunction(String token, int seed) // seed is the number of reducers -> number of workers as workers are both reducers and mappers
    {
        char firstChar = token.charAt(0);
        int charValue = firstChar - 'a';
        int allChars = 'z' - 'a' + 1;
        int winodwLength = (int)Math.ceil((float)allChars / (float)seed);
        int reducerId = charValue / winodwLength;

        return reducerId;
    }
}
