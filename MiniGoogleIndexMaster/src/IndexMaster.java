import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by Ahmed on 12/3/2016.
 */
public class IndexMaster
{
    public static void main(String args[]) throws Exception
    {
        ServerSocket s;
        s = new ServerSocket(0);
        int myPort = s.getLocalPort();
        s.close();


        String FilePath = "G:\\Work\\Java\\Work Space_4\\test.txt";
        String nameServerInfo = readNameServerCredentialsFromFile("G:\\Work\\Java\\Work Space_4\\ns.txt");
        String indexFolder = "G:\\Work\\Java\\Work Space_4\\Index\\IndPart";
        String[] parts = nameServerInfo.split("\n");
        int nameServerPort = new Integer(parts[0].trim());
        String nameServerIP = parts[1].trim();

        String result = requestWorkers(2,nameServerIP,String.valueOf(nameServerPort));
        result = result.replace("\n","").replace("\r","").trim();
        String[] serversInfo = result.split(";");
        HashMap<String, Boolean> taskDone = new HashMap<String, Boolean>();

        for(int i =0;i < serversInfo.length;i++)//String server :serversInfo)
        {
            taskDone.put(String.valueOf(i), false);
        }
        String workerPhoneBook = "";
        for(int i =0; i < serversInfo.length;i++)
        {
            workerPhoneBook+= i+" " + serversInfo[i] + ";";
        }
        workerPhoneBook = workerPhoneBook.substring(0,workerPhoneBook.length() - 1);
        for(int i =0; i < serversInfo.length;i++)
        {
            int workerID = i;
            String indexPath = indexFolder + i + ".txt";

            String workerIP = serversInfo[i].split("_")[0];
            String workerPort = serversInfo[i].split("_")[1];

            InitiateTask(myPort,i,String.valueOf(0), FilePath,String.valueOf(i * 800),"800",indexPath,workerIP,workerPort,serversInfo.length,workerPhoneBook);
        }
        Boolean finished = false;
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
                InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();
                String finishedWorkerIP = IPAddress.toString();
                finishedWorkerIP = finishedWorkerIP.replace("/","");
                String key = finishedWorkerIP + "_" + port;
                System.out.println( key + " Finished");

                String workerID = sentence.split(",")[0];
                taskDone.replace(workerID, true);
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

        for(String key :  serversInfo)
        {
            System.out.println("Terminating workers");
            String IP = key.split("_")[0];
            String Port = key.split("_")[1];
            terminateTask(IP,Port);
        }
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

    public static void InitiateTask(int portNumber, int workerID, String docID, String filePath,
                                    String startingIndex, String chunkLength, String indexPath,
                                    String workerIP, String workerPort, int numOfReducers, String phoneBook)
    {
        String message = "DW," + workerID + ","+ docID + "," + filePath+ "," + startingIndex + "," + chunkLength + "," + indexPath+","+numOfReducers + "," +phoneBook ;
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
}
