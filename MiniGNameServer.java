/**
 * Created by Ahmed on 11/28/2016.
 */

import java.io.*;
import java.net.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;

class MySocketThread implements Runnable {

    Socket cs;
    public MySocketThread(Socket _cs) {
        // store parameter for later user
        cs = _cs;
    }

    public void run()
    {
        CommunicateWithClient();
    }

    public void CommunicateWithClient()
    {
        try
        {
            PrintWriter out = new PrintWriter(cs.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(cs.getInputStream()));

            String inputLine;
            //String outputLine = "";
            //out.println(outputLine);

            String wholeText = "";
            wholeText+=in.readLine();
            //while ((inputLine = in.readLine()) != null)
            //	{
            //		wholeText+=inputLine + "\n";
            if (!wholeText.equals(null))
            {
                System.out.println(wholeText);
            }
            //	}
            wholeText = wholeText.replace("\n","").replace("\r","").trim();
            if(wholeText.charAt(0) == 'R')
            {
                boolean regResult = MiniGNameServer.RegisterWorker(wholeText);
                if(regResult == true)
                {
                    out.write("D\n");
                    out.flush();
                }
                else
                {
                    out.write("F\n");
                    out.flush();
                }
                //out.println("Done");
            }
            else if(wholeText.charAt(0) == 'G')
            {
                LinkedList<String> availableServers = MiniGNameServer.getListOfWorkers(wholeText);
                if(availableServers.size() == 0)
                {
                    System.out.println("No workers found");
                    out.write("NF\n");
                    out.flush();
                }
                else
                {
                    StringBuilder sB = new StringBuilder();
                    for(String s : availableServers)
                    {
                        sB.append(s);
                        sB.append(";");
                    }
                    out.write(sB.toString() + "\n");
                    out.flush();

                    //out.println(serverInfo);
                }

            }

            cs.close();
        }
        catch(Exception e)
        {


        }

    }


}

class workerStatus
{
    public String workerType = ""; // M->Mapper, R->Reducer, MR-> Mapper and Reducer
    public String workerCurrentStatus = "";   //I -> Idle, B->Busy
    public String ipAddress = "";
    public String portNumber = "";
    public int NumberofTimesUtilized = 0;
}

public class MiniGNameServer
{
    public static String filePath = "..//ns.txt";
    public static String backupFilePath = "..//backup.txt";

    public static HashMap<String, workerStatus> WorkersDictionary = new HashMap<String, workerStatus>();


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

    public static void writeCredentialsinFile(String FilePath, String IPAdd, int portNum)
    {
        try
        {
            Writer infoWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(FilePath), "utf-8"));

            infoWriter.write(portNum + "\n");
            infoWriter.write(IPAdd);

            infoWriter.close();

            infoWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("..//" + FilePath), "utf-8"));

            infoWriter.write(portNum + "\n");
            infoWriter.write(IPAdd);

            infoWriter.close();

        } catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void backupCurrentState(String FilePath)
    {
        try
        {
            Writer infoWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(FilePath), "utf-8"));

            StringBuilder sB = new StringBuilder();
            for(String s : WorkersDictionary.keySet())
            {
                sB.append(s);
                sB.append("\t");

                workerStatus wS = WorkersDictionary.get(s);
                sB.append(wS.ipAddress);
                sB.append("\t");
                sB.append(wS.portNumber);
                sB.append("\t");
                sB.append(wS.workerCurrentStatus);
                sB.append("\t");
                sB.append(wS.workerType);
                sB.append("\t");
                sB.append(wS.NumberofTimesUtilized);
                sB.append("\n");
            }
            infoWriter.write(sB.toString());
            infoWriter.close();

            infoWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("..//" + FilePath), "utf-8"));

            infoWriter.write(sB.toString());
            infoWriter.close();

        } catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void loadBackup(String FilePath)
    {
        String FileContent = "";
        String _filePath = FilePath;
        try
        {
            File tempFile = new File(_filePath);
            if(tempFile.exists())
            {

            }
            else
            {
                _filePath = "..//" + _filePath;
            }

            FileReader inputFile = new FileReader(_filePath);
            BufferedReader bufferReader = new BufferedReader(inputFile);
            String line;
            WorkersDictionary.clear();

            while ((line = bufferReader.readLine()) != null)
            {
                String[] strParts = line.replace("\n","").replace("\r","").trim().split("\t");

                workerStatus wS = new workerStatus();

                String keyStr = strParts[0].trim();
                wS.ipAddress = strParts[1].trim();
                wS.portNumber = strParts[2].trim();
                wS.workerCurrentStatus = strParts[3].trim();
                wS.workerType = strParts[4].trim();
                wS.NumberofTimesUtilized = Integer.valueOf(strParts[5].trim());
                WorkersDictionary.put(keyStr, wS);
            }
            bufferReader.close();
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }



    public static boolean RegisterWorker(String text)
    {
        try
        {
            String parts[] = text.split(",");

            String workerType = parts[1].trim();
            String Ip = parts[2].trim();
            String Port = parts[3].trim();

            String workerID = Ip + "_" + Port ;
            workerStatus wS = new workerStatus();
            wS.ipAddress = Ip;
            wS.portNumber = Port;
            wS.workerType = workerType;
            wS.workerCurrentStatus = "I";

            if (WorkersDictionary.containsKey(workerID))
            {
                //WorkersDictionary.replace(ServiceName.toLowerCase(), Ip + ":" + Port);
            }
            else
            {
                WorkersDictionary.put(workerID, wS);
            }
            if (workerType.equals("M"))
            {
                System.out.println("Worker with IP: " + Ip + ", and Port number " + Port + " is registered as Mapper");
            }
            else if(workerType.equals("R"))
            {
                System.out.println("Worker with IP: " + Ip + ", and Port number " + Port + " is registered as Reducer");
            }
            else if(workerType.equals("MR") || workerType.equals("RM") )
            {
                System.out.println("Worker with IP: " + Ip + ", and Port number " + Port + " is registered as Mapper and Producer");
            }
            return true;
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return false;
        }
    }


    public static String getServerInfo(String serverID)
    {
        try
        {
            if (WorkersDictionary.containsKey(serverID))
            {
                workerStatus wS = WorkersDictionary.get(serverID);
                return wS.workerCurrentStatus + "," + wS.workerType + "," + wS.NumberofTimesUtilized;
            }
            else
            {
                return "notfound";
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            return "notfound";
        }
    }

    public static LinkedList<String> getListOfWorkers(String text) // if number of workers == 0, return the all idle workers
    {
        String[] strParts = text.split(",");
        String workerType = strParts[1];
        int numberOfWorkers = Integer.valueOf(strParts[2]);

        LinkedList<String> workersNames = new LinkedList<String>();

        for (String s : WorkersDictionary.keySet())
        {
            if (WorkersDictionary.get(s).workerCurrentStatus.equals("I"))
            {
                int workerUtilization = WorkersDictionary.get(s).NumberofTimesUtilized;

                if (workersNames.size() == 0)
                {
                    workersNames.add(s);
                }
                else
                {
                    int insertIndex = 0;
                    for (int i = 0; i < workersNames.size(); i++)
                    {
                        if (WorkersDictionary.get(workersNames.get(i)).NumberofTimesUtilized >= workerUtilization)
                        {
                            workersNames.add(insertIndex,s);
                            break;
                        }
                        insertIndex++;
                    }
                }
            }
        }
        if(numberOfWorkers == 0 || workersNames.size() <= numberOfWorkers)
        {
            return workersNames;
        }
        else
        {
            return (LinkedList<String>)workersNames.subList(0,numberOfWorkers - 1);
        }
    }

    public static void main(String[] args)
    {
        // TODO Auto-generated method stub

        ServerSocket s;
        LinkedList<Thread> allThreads = new LinkedList<Thread>();
        try
        {
            s = new ServerSocket(0);
            int port = s.getLocalPort();
            String ip = getMyIP();

            writeCredentialsinFile(filePath, ip, port);
            while (true)
            {
                Socket clientSocket = new Socket();
                clientSocket = s.accept();

                Runnable r = new MySocketThread(clientSocket);


                Thread t = new Thread(r);
                allThreads.add(t);
                t.start();
                //CommunicateWithClient(clientSocket);

                for (int i = 0; i < allThreads.size(); i = i)
                {
                    if (allThreads.get(i).isAlive())
                    {
                        i++;
                    } else
                    {
                        allThreads.remove(i);
                    }

                }
            }


        } catch (Exception e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }


    }
}
