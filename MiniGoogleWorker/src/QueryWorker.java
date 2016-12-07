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
            //_parentWorker.recieveInformation();
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
                                    wordToDocuments.put(token, Parts[1].trim());
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

        private void countWords(HashMap<String,Integer> wordCount, String text)
        {
            Pattern p = Pattern.compile("([^0-9A-Za-z\\s])+",Pattern.DOTALL);
            text = text.replace("\n"," ").replace("\r"," ").replace(","," ").trim();
            //text = text.replaceAll("^([0-9A-Za-z]+)", " ");
            text =  p.matcher(text).replaceAll("");
            text = text.replaceAll("\\s+", " ");
            text = text.trim().toLowerCase();

            String[] tokens = text.split(" ");
            for(String token:tokens)
            {
                token = token.trim();
                if(wordCount.containsKey(token))
                {
                    int oldValue = wordCount.get(token);
                    oldValue++;
                    wordCount.replace(token,oldValue);
                }
                else
                {
                    wordCount.put(token, 1);
                }
            }
        }
    }

    class Reducer
    {
        /*public void Reduce(HashMap<String, String>  wordToDocuments)
        {
            for(String token : wordToDocuments)
            {

            }
            if(wordCountToWrite.containsKey(key))
            {
                int previousCount = wordCountToWrite.get(key);
                previousCount += count;
                wordCountToWrite.replace(key,count);
            }
            else
            {
                wordCountToWrite.put(key, count);
            }
        }
        */
    }

    QueryWorker.Mapper mP = new QueryWorker.Mapper();
    QueryWorker.Reducer rR = new QueryWorker.Reducer();

    String workerType = ""; // M, R, MR
    //int numberOfReducers = 0;
    //HashMap<Integer,String> reducerPhoneBook = new HashMap<Integer, String>();

    int ID;
    String myPortNumber = "";
    String myIPAddress = "";
    String _indexPath = "";

    String queryMasterIP = "";
    String queryMasterPortNumber = "";

    public QueryWorker(int myID, String _myIP, String _myPortNumber, String indexPath, String _queryMasterIP, String _queryMasterPortNumber)
    {
        ID = myID;
        _indexPath = indexPath;
        myIPAddress = _myIP;
        myPortNumber =  _myPortNumber;
        queryMasterIP = _queryMasterIP.replace("/","");
        queryMasterPortNumber = _queryMasterPortNumber;
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
        //Runnable r = new QueryWorker.MySocketThread(this);
        //Thread recieveDataThread = new Thread(r);

        //Thread recieveDataThread = new Thread(){public void run(){try{recieveInformation();} catch(Exception v) {}}};
        //recieveDataThread.start();

        HashMap<String,String> wordToDocs = mP.Map( query, _indexPath);
        for(String word : wordToDocs.keySet())
        {
            sendInformation(word, wordToDocs.get(word), queryMasterIP, queryMasterPortNumber);
        }

        // send Index master that reducerFinishedWorking
        communicateWithQueryMaster();

        // wait for index master to termintate the process
        try
        {
            //recieveDataThread.join();
        }
        catch(Exception e)
        {
            System.out.println("error in waiting");
        }

        //System.out.println("Finished Execution");
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

    /*public void recieveInformation()
    {
        try
        {
            DatagramSocket serverSocket = new DatagramSocket(Integer.valueOf(myPortNumber));
            while (true)
            {
                byte[] receiveData = new byte[64];
                byte[] sendData = new byte[64];
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                String sentence = new String(receivePacket.getData());
                sentence = sentence.replace("\n","").replace("\r","").trim();

                System.out.println("Recieved " + sentence);

                if(sentence.equals("endprocess"))
                {
                    System.out.println("recieved finishing command from index master");
                    serverSocket.close();
                    return;
                }
                String[] parts = sentence.split(":");
                String word = parts[0];
                String number = parts[1].replace("\n","").replace("\r","").replace("\\","").replace("/","").trim();
                int count = Integer.valueOf(number);

                //System.out.println("Recieved word "+ word + ":" + count);
                rR.Reduce(word, count);
                InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();

                sendData = "ack".getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
                serverSocket.send(sendPacket);
                //serverSocket.close();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println("exception");
        }
    }*/
}
