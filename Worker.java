import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.io.*;
import java.net.*;

/**
 * Created by Ahmed on 11/29/2016.
 */
public class Worker
{
    class Mapper
    {
        public  HashMap<String,Integer> Map(int startingIndex, int chunkLength, String textFilePath)
        {
            StringBuilder Content = new StringBuilder();
            String _filePath = textFilePath;
            try
            {
                File tempFile = new File(_filePath);
                if(tempFile.exists())
                {
                    FileReader inputFile = new FileReader(_filePath);
                    BufferedReader bufferReader = new BufferedReader(inputFile);

                    char[] specifiedPart = new char[chunkLength];
                    bufferReader.read(specifiedPart,startingIndex,chunkLength);

                    for(char c : specifiedPart)
                    {
                        Content.append(c);
                    }

                    HashMap<String,Integer> wordCount = new HashMap<String,Integer>();
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
            text = text.replace("\n"," ").replace("\r"," ").trim();
            text = text.replaceAll("\\^([0-9A-Za-z]+)", " ");
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
               wordCountToWrite.replace(key,count);
           }
            else
           {
               wordCountToWrite.put(key, count);
           }
        }

    }

    Mapper mP = new Mapper();
    Reducer rR = new Reducer();

    String workerType = ""; // M, R, MR
    int numberOfReducers = 0;
    HashMap<Integer,String> reducerPhoneBook = new HashMap<Integer, String>();
    int ID;
    int myPortNumber = 0;
    String myIPAddress = "";

    public int hashFunction(String token, int seed)
    {
        char firstChar = token.charAt(0);
        int charValue = firstChar - 'a';
        int allChars = 'z' - 'a' + 1;
        int winodwLength = (int)Math.ceil((float)allChars / (float)seed);
        int reducerId = charValue / winodwLength;

        return reducerId;
    }

    public void execute(int startingIndex, int chunkLength, String textFilePath)
    {
        HashMap<String,Integer> wordCount = mP.Map( startingIndex,  chunkLength,  textFilePath);
        for(String word : wordCount.keySet())
        {
            int reducerId = hashFunction(word, numberOfReducers);
            communicateWithReducer(reducerId, word, wordCount.get(word));
        }

        //Write In index, and wait for index master to termintate the process

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

            String[] parts = reducerEntryPoint.split(";");
            String ipAddress = parts[0].trim();
            String portNumber = parts[1].trim();

            sendInformation(word,currentWordCount,ipAddress,portNumber);
        }
    }

    public void sendInformation(String word, int currentWordCount, String reducerIP, String reducerPortNumber)
    {
        try
        {
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData = new byte[64];
            byte[] receiveData = new byte[64];

            String message = word + ":" + currentWordCount;
            sendData = message.getBytes();
            InetAddress IPAddress = InetAddress.getByAddress(reducerIP.getBytes());
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.valueOf(reducerPortNumber));


            clientSocket.send(sendPacket);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);
            String reply = new String(receivePacket.getData());
            if(reply.toLowerCase().equals("ack"))
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
            DatagramSocket serverSocket = new DatagramSocket(myPortNumber);
            byte[] receiveData = new byte[64];
            byte[] sendData = new byte[64];
            while (true)
            {
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                serverSocket.receive(receivePacket);
                String sentence = new String(receivePacket.getData());
                if(sentence.equals("endprocess"))
                {
                    break;
                }
                String[] parts = sentence.split(":");
                String word = parts[0];
                int count = Integer.valueOf(parts[1]);
                rR.Reduce(word, count);
                InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();

                sendData = "ack".getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
                serverSocket.send(sendPacket);
            }
        }
        catch(Exception e)
        {

        }
    }
}
