import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.io.*;
import java.net.*;
import java.util.regex.Pattern;

/**
 * Created by Ahmed on 11/29/2016.
 */
public class IndexingWorker
{
    class MySocketThread implements Runnable
    {
        IndexingWorker _parentWorker;
        public MySocketThread(IndexingWorker parentWorker) {
            // store parameter for later user
            _parentWorker = parentWorker;
        }

        public void run()
        {
            _parentWorker.recieveInformation();
        }

    }
    class Mapper
    {
        public  HashMap<String,Integer> Map(int startingIndex, int chunkLength, String textFilePath)
        {
            StringBuilder Content = new StringBuilder();
            String _filePath = textFilePath;
            try
            {
                File tempFile = new File(_filePath);
                if (tempFile.exists())
                {
                    FileReader inputFile = new FileReader(_filePath);
                    BufferedReader bufferReader = new BufferedReader(inputFile);

                    try
                    {
                        int count = 0;
                        int intch;
                        while (((intch = bufferReader.read()) != -1) && count < startingIndex)
                        {
                            count++;
                        }
                        count = 0;
                        while (((intch = bufferReader.read()) != -1) && count < chunkLength)
                        {
                            Content.append((char) intch);
                            count++;
                        }
                    } finally
                    {
                        //bufferReader.close();
                    }

                    HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
                    countWords(wordCount, Content.toString());
                    bufferReader.close();
                    return wordCount;
                }
            }
            catch (Exception e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            return new HashMap<String,Integer>();
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
        HashMap<String,Integer> wordCountToWrite = new HashMap<String, Integer>();

        public void Reduce(String key, int count)
        {
            if(wordCountToWrite.containsKey(key))
            {
                int previousCount = wordCountToWrite.get(key);
                previousCount += count;
                wordCountToWrite.replace(key,previousCount);
            }
            else
            {
                wordCountToWrite.put(key, count);
            }
        }

        public void writeDataInIndex(String indexPath, String DocID)
        {
            String IndexFolder = indexPath;

            for(String word : wordCountToWrite.keySet())
            {
                String _filePath = IndexFolder + "\\" + word.charAt(0) + "\\" + word.charAt(0) + "_Index.txt";
                try
                {
                    writeWordToIndex(word,wordCountToWrite.get(word),DocID,_filePath);
                } catch (Exception e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        public void writeWordToIndex(String wordToWrite, int wordToWriteCount, String DocID, String indexPath)
        {
            String _filePath = indexPath;
            try
            {
                File tempFile = new File(_filePath);
                if(tempFile.exists())
                {
                    FileReader inputFile = new FileReader(_filePath);
                    BufferedReader bufferReader = new BufferedReader(inputFile);

                    StringBuilder sB = new StringBuilder();
                    String line = bufferReader.readLine();
                    while (line != null)
                    {
                        line = line.replace("\r", "").replace("\n", "").trim();
                        if (!line.equals(""))
                        {
                            String[] Parts = line.split("\t");
                            String word = Parts[0];
                            String docs = Parts[1];

                            if (wordToWrite.equals(word))
                            {
                                String[] allDocs = docs.split(";");
                                HashMap<String, Integer> wordCountInDocs = new HashMap<String, Integer>();

                                for (int i = 0; i < allDocs.length; i++)
                                {
                                    String[] docInfo = allDocs[i].trim().split(",");
                                    String docId = docInfo[0].trim();
                                    String wordCount = docInfo[1].trim();
                                    wordCountInDocs.put(docId, Integer.valueOf(wordCount));
                                }

                                if (wordCountInDocs.containsKey(DocID))
                                {

                                }
                                else
                                {
                                    //infoWriter.write(word + "\t" + docs + ";" + DocID + "," + wordToWriteCount + "\n");
                                    sB.append(word + "\t" + docs + ";" + DocID + "," + wordToWriteCount + "\n");
                                }
                                //break;
                            }
                            else
                            {
                                //infoWriter.write(word + "\t" + docs + "\n");
                                sB.append(word + "\t" + docs + "\n");
                            }
                        }
                        line = bufferReader.readLine();
                    }
                    bufferReader.close();
                    // Write RemainingWords
                    Writer infoWriter = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(_filePath), "utf-8"));
                    infoWriter.write(sB.toString());
                    infoWriter.write(wordToWrite + "\t" + DocID + "," + wordToWriteCount + "\n");
                    infoWriter.close();
                }
                else
                {
                    Writer infoWriter = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(_filePath), "utf-8"));
                    infoWriter.write(wordToWrite + "\t" + DocID + "," + wordToWriteCount + "\n");
                    infoWriter.close();
                }
            }
            catch (Exception e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    Mapper mP = new Mapper();
    Reducer rR = new Reducer();

    String workerType = ""; // M, R, MR
    int numberOfReducers = 0;
    HashMap<Integer,String> reducerPhoneBook = new HashMap<Integer, String>();

    int ID;
    String myPortNumber = "";
    String myIPAddress = "";
    String _indexPath = "";

    String indexMasterIP = "";
    String indexMasterPortNumber = "";

    public IndexingWorker(int myID, String _myIP, String _myPortNumber, String indexPath, String _indexMasterIP, String _indexMasterPortNumber, int _numberOfReducers, String reducersInformation)
    {
        ID = myID;
        _indexPath = indexPath;
        myIPAddress = _myIP;
        myPortNumber =  _myPortNumber;
        indexMasterIP = _indexMasterIP.replace("/","");
        indexMasterPortNumber = _indexMasterPortNumber;
        numberOfReducers =_numberOfReducers;

        String[] reducers = reducersInformation.split(";");

        for(String red : reducers)
        {
            String parts[] = red.split(" ");
            String id = parts[0];
            String entryPoint = parts[1];
            reducerPhoneBook.put(Integer.valueOf(id), entryPoint);
        }
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

    public void execute(String DocID, int startingIndex, int chunkLength, String textFilePath)
    {
        Runnable r = new MySocketThread(this);
        Thread recieveDataThread = new Thread(r);

        //Thread recieveDataThread = new Thread(){public void run(){try{recieveInformation();} catch(Exception v) {}}};
        recieveDataThread.start();
        HashMap<String,Integer> wordCount = mP.Map( startingIndex,  chunkLength,  textFilePath);
        for(String word : wordCount.keySet())
        {
            int reducerId = hashFunction(word, numberOfReducers);
            communicateWithReducer(reducerId, word, wordCount.get(word));
        }

        // send Index master that reducerFinishedWorking
        communicateWithIndexMaster();

        // wait for index master to termintate the process
        try
        {
            recieveDataThread.join();
        }
        catch(Exception e)
        {
            System.out.println("error in waiting");
        }

        //Write In index
        rR.writeDataInIndex(_indexPath,DocID);

        //System.out.println("Finished Execution");
    }

    public void communicateWithIndexMaster()
    {
        try
        {
            DatagramSocket clientSocket = new DatagramSocket();//Integer.valueOf(myPortNumber));
            byte[] sendData = new byte[64];
            byte[] receiveData = new byte[64];

            String message = String.valueOf(ID) + ",Done";
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(indexMasterIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(indexMasterPortNumber));


            clientSocket.send(sendPacket);
            //DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            //clientSocket.receive(receivePacket);
            //String reply = new String(receivePacket.getData());
            //if(reply.toLowerCase().equals("ack"))
            //{
            clientSocket.close();
            //}
            System.out.println("Finished Inexing");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    public void communicateWithReducer(int reducerID, String word, int currentWordCount)
    {
        if (reducerID == ID)
        {
            rR.Reduce(word, currentWordCount);
        }
        else
        {
            String reducerEntryPoint = reducerPhoneBook.get(reducerID);

            String[] parts = reducerEntryPoint.split("_");
            String ipAddress = parts[0].trim();
            String portNumber = parts[1].trim();

            sendInformation(word,currentWordCount,ipAddress,portNumber);
        }
    }

    public void sendInformation(String word, int currentWordCount, String reducerIP, String reducerPortNumber)
    {
        try
        {
            DatagramSocket clientSocket = new DatagramSocket();//Integer.valueOf(myPortNumber));
            byte[] sendData = new byte[64];
            byte[] receiveData = new byte[64];

            String message = word + ":" + currentWordCount;
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByName(reducerIP);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(reducerPortNumber));


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

    public void recieveInformation()
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
    }

}

