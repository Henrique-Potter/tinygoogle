//import com.sun.java.util.jar.pack.ConstantPool;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by Ahmed on 12/7/2016.
 */
public class MiniGoogleServer
{
    static HashMap<String, String> docIDsToPath = new HashMap<String, String>();
    static HashMap<String, Thread> RunningThreadsAndIDs = new HashMap<String, Thread>();
    static HashMap<String, Boolean> RunningThreadsStatus = new HashMap<String, Boolean>();

    static String indexingPath = "G:\\Work\\Java\\Work Space_4\\Index";
    static String searchingPath = "G:\\Work\\Java\\Work Space_4\\Index_back";
    static boolean currentlyIndexing = false;

    static class MySocketThread implements Runnable
    {
        Socket cs;
        QueryMaster qM = null;
        IndexMaster iM = null;
        String query = "";
        String filePath = "";

        public MySocketThread(Socket _cs, QueryMaster _qM, String _query)
        {
            // store parameter for later user
            cs = _cs;
            iM = null;
            qM = _qM;
            query = _query;
        }

        public MySocketThread(Socket _cs, IndexMaster _iM, String _filePath)
        {
            // store parameter for later user
            cs = _cs;
            iM = _iM;
            qM = null;
            filePath = _filePath;
        }

        public void run()
        {
            if (qM == null && iM != null)
            {
                // Index Thread
                currentlyIndexing = true;
                iM.RunIndexService();
                switchIndex();
                saveDocumentsInfo();
                try
                {
                    PrintWriter out = new PrintWriter(cs.getOutputStream(), true);
                    out.write("Indexing Done" + "\n");
                    out.flush();
                    cs.close();

                } catch (Exception e)
                {
                    e.printStackTrace();
                }
                iM.releaseWorkers();
                currentlyIndexing = false;
            }
            else if (iM == null && qM != null)
            {
                //Query Thread
                qM.RunQueryService();
                HashMap<String, Integer> resultDocIDs = qM.getQueryResult();
                StringBuilder replyMessage = new StringBuilder();
                for (String key : resultDocIDs.keySet())
                {
                    if (docIDsToPath.containsKey(key))
                    {
                        replyMessage.append(docIDsToPath.get(key));
                        replyMessage.append("\t");
                    }
                }
                try
                {
                    PrintWriter out = new PrintWriter(cs.getOutputStream(), true);
                    out.write(replyMessage.toString().trim() + "\n");
                    out.flush();
                    cs.close();
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
                qM.releaseWorkers();
            }
        }
    }

    public static void main_(String args[]) throws Exception
    {
        testServer();
    }
    public static void _main(String args[]) throws Exception
    {

        loadDocumentsInfo();
        ServerSocket s = null;
        try
        {
            s = new ServerSocket(0);
            int port = s.getLocalPort();
            String myIPAddress = s.getInetAddress().getHostName().replace("/","").trim();
            LinkedList<Thread> allThreads = new LinkedList<Thread>();

            while (true)
            {
                Socket clientSocket = new Socket();
                clientSocket = s.accept();
                String message = readMessageFromClient(clientSocket);
                if(message.substring(0,2).equals("S,"))
                {
                    // search Query
                    String query = message.replace("\n","").replace("\r","").trim().split(",")[1];
                    QueryMaster qM = new QueryMaster(query,searchingPath);
                    Runnable r = new MySocketThread(clientSocket,qM,searchingPath);
                    Thread t = new Thread(r);
                    t.start();
                }
                else if(message.substring(0,2).equals("I,"))
                {
                    // Index Query
                    if (currentlyIndexing == true)
                    {
                        // reply with currently indexing
                        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                        out.write("Indexing Done" + "\n");
                        out.flush();
                        clientSocket.close();
                    }
                    else
                    {
                        String filePath = message.replace("\n", "").replace("\r", "").trim().split(",")[1].trim();
                        if(checkIndexedBefore(filePath) == false)
                        {
                            int NewFileID = docIDsToPath.keySet().size();
                            while (docIDsToPath.containsKey(String.valueOf(NewFileID)))
                            {
                                NewFileID += 1;
                            }
                            docIDsToPath.put(String.valueOf(NewFileID), filePath);
                            IndexMaster iM = new IndexMaster(filePath, NewFileID, indexingPath);
                            Runnable r = new MySocketThread(clientSocket, iM, filePath);
                            Thread t = new Thread(r);
                            t.start();
                        }
                    }
                }
                /*Runnable r = new MySocketThread(clientSocket);
                Thread t = new Thread(r);
                allThreads.add(t);
                t.start();

                for (int i = 0; i < allThreads.size(); i = i)
                {
                    if (allThreads.get(i).isAlive())
                    {
                        i++;
                    } else
                    {
                        allThreads.remove(i);
                    }

                }*/
            }


        } catch (Exception e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            if (s != null)
            {
                try
                {
                    s.close();
                } catch (Exception e)
                {
                    s = null;
                }
            }
        }
    }

    public static void saveDocumentsInfo()
    {
        try
        {
            Writer infoWriter = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("dataIndexed"), "utf-8"));

            StringBuilder sB = new StringBuilder();
            for(String s : docIDsToPath.keySet())
            {
                sB.append(s);
                sB.append("\t");
                sB.append(docIDsToPath.get(s));
                sB.append("\n");
            }
            infoWriter.write(sB.toString());
            infoWriter.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void loadDocumentsInfo()
    {
        try
        {
            File tempFile = new File("dataIndexed");
            if (tempFile.exists())
            {
                FileReader inputFile = new FileReader("dataIndexed");
                BufferedReader bufferReader = new BufferedReader(inputFile);
                String line;
                docIDsToPath.clear();

                while ((line = bufferReader.readLine()) != null)
                {
                    String[] strParts = line.replace("\n", "").replace("\r", "").trim().split("\t");

                    String keyStr = strParts[0].trim();
                    String val = strParts[1].trim();

                    docIDsToPath.put(keyStr,val);
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void switchIndex()
    {
        String temp = indexingPath;
        indexingPath = searchingPath;
        searchingPath = temp;
    }
    public static void copyIndex()
    {

    }
    public static void testServer()
    {
        try
        {
            boolean succes = false;
            /*IndexMaster iM = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt",14,indexingPath);
            succes = iM.RunIndexService();
            System.out.println("Done Indexing 1 " + succes);
            Thread.sleep(1000);

            IndexMaster iM2 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt",15,indexingPath);
            succes = iM2.RunIndexService();
            System.out.println("Done Indexing 2" + succes);
            Thread.sleep(1000);

            IndexMaster iM3 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt", 16,indexingPath);
            succes = iM3.RunIndexService();
            System.out.println("Done Indexing 3" + succes);
            Thread.sleep(1000);

            IndexMaster iM4 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt", 17,indexingPath);
            succes = iM4.RunIndexService();
            System.out.println("Done Indexing 4" + succes);
            Thread.sleep(1000);

            IndexMaster iM5 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt", 18,indexingPath);
            succes = iM5.RunIndexService();
            System.out.println("Done Indexing 5" + succes);
            */

            Thread.sleep(1000);
            String Query = "I hate hate hate hate people people";
            QueryMaster qM = new QueryMaster(Query,indexingPath);
            succes = qM.RunQueryService();
            HashMap<String, Integer> docIDs = qM.getQueryResult();
            System.out.println("Done Searching first time " + succes);

            Thread.sleep(1000);
            QueryMaster qM2 = new QueryMaster(Query,indexingPath);
            succes = qM2.RunQueryService();
            docIDs = qM2.getQueryResult();
            System.out.println("Done Searching second time " + succes);

            Thread.sleep(1000);
            QueryMaster qM3 = new QueryMaster(Query,indexingPath);
            succes = qM3.RunQueryService();
            docIDs = qM3.getQueryResult();
            System.out.println("Done Searching third time " + succes);

            Thread.sleep(1000);
            QueryMaster qM4 = new QueryMaster(Query,indexingPath);
            succes = qM2.RunQueryService();
            docIDs = qM2.getQueryResult();
            System.out.println("Done Searching fourth time " + succes);

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static String readMessageFromClient(Socket cs)
    {
        try
        {
            BufferedReader in = new BufferedReader(new InputStreamReader(cs.getInputStream()));

            String inputLine;
            String wholeText = "";
            wholeText = in.readLine();
            System.out.println("Client: " + wholeText);
            return wholeText;
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        return "";
    }

    public static boolean checkIndexedBefore(String filePath)
    {
        for(String key: docIDsToPath.keySet())
        {
            if(docIDsToPath.get(key).equals(filePath))
            {
                return true;
            }
        }
        return false;
    }
}
