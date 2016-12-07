import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Created by Ahmed on 12/6/2016.
 */
public class QueryWorker
{
    class MySocketThread implements Runnable
    {
        QueryWorker _parentWorker;
        public MySocketThread(QueryWorker parentWorker) {
            // store parameter for later user
            _parentWorker = parentWorker;
        }

        public void run()
        {
            _parentWorker.heartBeat();
        }

    }

    class Mapper
    {
        public HashMap<String, String> Map(String query, String indexFolder)
        {
            HashMap<String, String> wordToDocuments = new HashMap<String, String>();
            String[] words = query.split(" ");
            for(String token:words)
            {
                String _filePath = indexFolder + "\\" + token.charAt(0) + "\\" + token.charAt(0) + "_Index.txt";
                wordToDocuments.put(token,"");
                try
                {
                    File tempFile = new File(_filePath);
                    if (tempFile.exists())
                    {
                        FileReader inputFile = new FileReader(_filePath);
                        BufferedReader bufferReader = new BufferedReader(inputFile);
                        String Line = bufferReader.readLine();

                        while (Line != null)
                        {
                            Line = Line.replace("\n", "").replace("\r", "").trim();
                            if (!Line.equals(""))
                            {
                                String[] Parts = Line.split("\t");
                                if (Parts[0].trim().equals(token))
                                {
                                    wordToDocuments.replace(token, Parts[1].trim());
                                    break;
                                }
                            }
                            Line = bufferReader.readLine();
                        }
                        bufferReader.close();
                    }
                }
                catch (Exception e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            return wordToDocuments;
        }
    }

    QueryWorker.Mapper mP = new QueryWorker.Mapper();

    String workerType = ""; // M, R, MR

    int ID;
    String myPortNumber = "";
    String myIPAddress = "";
    String _indexPath = "";

    String queryMasterIP = "";
    String queryMasterPortNumber = "";
    boolean endThread = false;
    int sendingHeartBEatFrequency = 2000;

    public QueryWorker(int myID, String _myIP, String _myPortNumber, String indexPath, String _queryMasterIP, String _queryMasterPortNumber)
    {
        ID = myID;
        _indexPath = indexPath;
        myIPAddress = _myIP;
        myPortNumber =  _myPortNumber;
        queryMasterIP = _queryMasterIP.replace("/","");
        queryMasterPortNumber = _queryMasterPortNumber;
        endThread = false;
    }

    public int hashFunction(String token, int seed) // seed is the number of reducers -> number of workers as workers are both reducers and mappers
    {
        char firstChar = token.charAt(0);
        int charValue = firstChar - 'a';
        int allChars = 'z' - 'a' + 1;
        int winodwLength = (int)Math.ceil((float)allChars / (float)seed);
        int reducerId = charValue / winodwLength;

        return reducerId;
    }

    public void execute(String query)
    {
        try
        {
            Runnable r = new QueryWorker.MySocketThread(this);
            Thread heartBeatThread = new Thread(r);

            //Thread recieveDataThread = new Thread(){public void run(){try{recieveInformation();} catch(Exception v) {}}};
            heartBeatThread.start();

            HashMap<String, String> wordToDocs = mP.Map(query, _indexPath);
            for (String word : wordToDocs.keySet())
            {
                sendInformation(word, wordToDocs.get(word), queryMasterIP, queryMasterPortNumber);
            }

            // send Index master that reducerFinishedWorking
            communicateWithQueryMaster();

            terminateThread();
            //System.out.println("Finished Execution");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public void communicateWithQueryMaster()
    {
        try
        {
            DatagramSocket clientSocket = new DatagramSocket();//Integer.valueOf(myPortNumber));
            byte[] sendData = new byte[64];
            byte[] receiveData = new byte[64];

            String message = String.valueOf(ID) + ",Done";
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(queryMasterIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(queryMasterPortNumber));


            clientSocket.send(sendPacket);
            //DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            //clientSocket.receive(receivePacket);
            //String reply = new String(receivePacket.getData());
            //if(reply.toLowerCase().equals("ack"))
            //{
            clientSocket.close();
            //}
            System.out.println("Finished searching");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    public void sendInformation(String word, String wordToDocs, String destinationIP, String destinationPortNumber)
    {
        try
        {
            DatagramSocket clientSocket = new DatagramSocket();//Integer.valueOf(myPortNumber));
            byte[] sendData = new byte[64];
            byte[] receiveData = new byte[1024];

            String message = String.valueOf(ID) + ":" + word + ":" + wordToDocs;
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(destinationIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(destinationPortNumber));


            clientSocket.send(sendPacket);
            //DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            //clientSocket.receive(receivePacket);
            //String reply = new String(receivePacket.getData());
            //reply = reply.replace("\n","").replace("\r","").trim();
            //if(reply.toLowerCase().equals("ack"))
            {
                clientSocket.close();
            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    public void heartBeat()
    {
        try
        {
            DatagramSocket serverSocket = new DatagramSocket();
            while (!endThread)
            {
                byte[] sendData = new byte[64];
                String message = "HBM,"+String.valueOf(ID);
                sendData = message.getBytes();
                InetAddress IPAddress = InetAddress.getByName(queryMasterIP);
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(queryMasterPortNumber));
                serverSocket.send(sendPacket);

                Thread.sleep(sendingHeartBEatFrequency);
            }
            serverSocket.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public void terminateThread()
    {
        endThread = true;
    }

}
