/**
 * Created by Ahmed on 12/3/2016.
 */
import java.io.*;
import java.net.*;
import java.util.Enumeration;

public class WorkerRunner
{
    static class myHeartBeatThread implements Runnable
    {
        String MyIP = "";
        String MyPort = "";
        String nsPort = "";
        String nsIP = "";
        public myHeartBeatThread(String myIP, String myPort, String nameServerIP, String NameServerPortNumber) {
            // store parameter for later user
            MyIP = myIP;
            MyPort = myPort;
            nsIP = nameServerIP;
            nsPort = NameServerPortNumber;
        }

        public void run()
        {
            while(true)
            {
                try
                {
                    Socket sc = new Socket(nsIP, Integer.valueOf(nsPort));
                    PrintWriter out = new PrintWriter(sc.getOutputStream(), true);
                    out.write("W," + MyIP + "," +MyPort+ "\n");
                    out.flush();
                    sc.close();
                    Thread.sleep(5000);
                }catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String args[]) throws Exception
    {

        String nameServerInfo = readNameServerCredentialsFromFile("G:\\Work\\Java\\Work Space_4\\ns.txt");
        String[] parts = nameServerInfo.split("\n");
        int nameServerPort = new Integer(parts[0].trim());
        String nameServerIP = parts[1].trim();



        DatagramSocket serverSocket = new DatagramSocket(0);
        int myPort = serverSocket.getLocalPort();
        String myIp = getMyIP();
        RegisterWithNameServer(nameServerIP, nameServerPort, myIp,myPort);

        Runnable r = new myHeartBeatThread(myIp, String.valueOf(myPort), nameServerIP, String.valueOf(nameServerPort));
        Thread t = new Thread(r);
        t.start();

        while(true)
        {
            byte[] receiveData = new byte[2048];
            if (serverSocket.isClosed())
            {
                serverSocket = new DatagramSocket(myPort);
            }
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            serverSocket.receive(receivePacket);
            String sentence = new String(receivePacket.getData());
            sentence = sentence.replace("\n","").replace("\r","").trim();
            if(sentence.substring(0,4).equals("DIW,"))
            {
                System.out.println("RECEIVED: " + sentence);
                sentence = sentence.substring(4);
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


                serverSocket.close();
                IndexingWorker currentWorker = new IndexingWorker(workerID, myIp, String.valueOf(myPort), indexPath,
                        indexMasterIP, String.valueOf(indexMasterPortNumber), numberOfReducers, reducersPhoneBook);

                currentWorker.execute(docID, Integer.valueOf(startingIndex), Integer.valueOf(chunkLength), filePath);

                System.out.println("Finished Execution");

                //String capitalizedSentence = sentence.toUpperCase();
                //sendData = capitalizedSentence.getBytes();
                //DatagramPacket sendPacket =
                //        new DatagramPacket(sendData, sendData.length, IPAddress, port);
                //serverSocket.send(sendPacket);
            }
            else if(sentence.substring(0,4).equals("DQW,"))
            {
                System.out.println("RECEIVED Query: " + sentence);
                sentence = sentence.substring(4);
                InetAddress IPAddress = receivePacket.getAddress();
                int port = receivePacket.getPort();

                String queryMasterIP = IPAddress.toString();
                int queryMasterPortNumber = port;

                String[] requestParts = sentence.split(",");

                int workerID = Integer.valueOf(requestParts[0]);
                String indexPath = requestParts[1];
                int numberOfReducers = Integer.valueOf(requestParts[2]);
                String query = requestParts[3].trim();
                serverSocket.close();

                QueryWorker currentWorker = new QueryWorker(workerID, myIp, String.valueOf(myPort), indexPath, queryMasterIP, String.valueOf(queryMasterPortNumber));

                currentWorker.execute(query);

                System.out.println("Finished Execution");
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
                System.out.println("IndexingWorker Registered");
            }
            else if (wholeText.trim().replace("\r", "").replace("\n", "").toUpperCase().equals("F"))
            {
                System.out.println("IndexingWorker Registration Failed");
            }
            //--------------------------------
            registrationSocket.close();
        }
        catch(Exception e)
        {

        }
    }
}
