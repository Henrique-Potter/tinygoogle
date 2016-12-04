/**
 * Created by Ahmed on 12/3/2016.
 */
import java.io.*;
import java.net.*;
import java.util.Enumeration;

public class WorkerRunner
{
    public static void main(String args[]) throws Exception
    {
        String nameServerInfo = readNameServerCredentialsFromFile("G:\\Work\\Java\\Work Space_4\\ns.txt");
        String[] parts = nameServerInfo.split("\n");
        int nameServerPort = new Integer(parts[0].trim());
        String nameServerIP = parts[1].trim();


        DatagramSocket serverSocket = new DatagramSocket(0);
        byte[] receiveData = new byte[1024];
        int myPort = serverSocket.getLocalPort();
        String myIp = getMyIP();
        RegisterWithNameServer(nameServerIP, nameServerPort, myIp,myPort);

        while(true)
        {
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            serverSocket.receive(receivePacket);
            String sentence = new String(receivePacket.getData());
            System.out.println("RECEIVED: " + sentence);
            if(sentence.substring(0,3).equals("DW,"))
            {
                sentence = sentence.substring(3);
                InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();

                String indexMasterIP = IPAddress.toString();
                int indexMasterPortNumber = port;

                String[] requestParts = sentence.split(",");

                int workerID = Integer.valueOf(requestParts[0]);
                String docID = requestParts[1];
                String filePath = requestParts[2];
                String startingIndex = requestParts[3];
                String chunkLength = requestParts[4];
                String indexPath = requestParts[5];
                int numberOfReducers = Integer.valueOf(requestParts[6]);
                String reducersPhoneBook = requestParts[7];

                Worker currentWorker = new Worker(workerID, myIp, String.valueOf(myPort), indexPath,
                        indexMasterIP, String.valueOf(indexMasterPortNumber), numberOfReducers, reducersPhoneBook);

                currentWorker.execute(docID, Integer.valueOf(startingIndex), Integer.valueOf(chunkLength), filePath);
            }
        }
    }

    public static String getMyIP()
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

    public static void RegisterWithNameServer(String nameServerIP, int nameServerPort, String myIP, int myPortNumber)
    {
        try
        {
            Socket registrationSocket = new Socket(nameServerIP, nameServerPort);
            PrintWriter out = new PrintWriter(registrationSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(registrationSocket.getInputStream()));
            out.write("R,MR,"+ myIP + "," + myPortNumber + "\n");
            out.flush();

            //--------------Get Response------------------
            String inputLine;
            String wholeText = "";
            wholeText = in.readLine();
            if (wholeText.trim().replace("\r", "").replace("\n", "").toUpperCase().equals("D"))
            {
                System.out.println("Worker Registered");
            }
            else if (wholeText.trim().replace("\r", "").replace("\n", "").toUpperCase().equals("F"))
            {
                System.out.println("Worker Registration Failed");
            }
            //--------------------------------
            registrationSocket.close();
        }
        catch(Exception e)
        {

        }
    }
}
