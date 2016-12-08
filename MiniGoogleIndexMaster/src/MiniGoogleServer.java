//import com.sun.java.util.jar.pack.ConstantPool;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by Ahmed on 12/7/2016.
 */
public class MiniGoogleServer
{
    class MySocketThread implements Runnable {
        Socket cs;
        QueryMaster qM = null;
        IndexMaster iM = null;
        String query = "";
        String filePath = "";
        public MySocketThread(Socket _cs, QueryMaster _qM, String _query) {
            // store parameter for later user
            cs = _cs;
            iM = null;
            qM = _qM;
            query = _query;
        }

        public MySocketThread(Socket _cs, IndexMaster _iM, String _filePath) {
            // store parameter for later user
            cs = _cs;
            iM = _iM;
            qM = null;
            filePath = _filePath;
        }

        public void run()
        {
            if(qM == null && iM != null)
            {

            }
            else if(iM == null && qM != null)
            {

            }
        }

    }

    public static void main(String args[]) throws Exception
    {

        /*ServerSocket s = null;
        try
        {
            s = new ServerSocket(0);
            int port = s.getLocalPort();
            ServerStub.registerServices(port);
            LinkedList<Thread> allThreads = new LinkedList<Thread>();

            while (true)
            {
                Socket clientSocket = new Socket();
                clientSocket = s.accept();

                Runnable r = new MySocketThread(clientSocket);
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

                }
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
*/
        boolean succes = false;
        IndexMaster iM = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt",2, 14);
        succes = iM.RunIndexService();
        System.out.println("Done Indexing 1 " + succes);
        Thread.sleep(1000);
        IndexMaster iM2 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt",2, 15);
        succes = iM2.RunIndexService();
        System.out.println("Done Indexing 2" + succes);
        Thread.sleep(1000);
        IndexMaster iM3 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt",2, 16);
        succes = iM3.RunIndexService();
        System.out.println("Done Indexing 3" + succes);
        Thread.sleep(1000);
        IndexMaster iM4 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt",2, 17);
        succes = iM4.RunIndexService();
        System.out.println("Done Indexing 4" + succes);
        Thread.sleep(1000);
        IndexMaster iM5 = new IndexMaster("G:\\Work\\Java\\Work Space_4\\test.txt",2, 18);
        succes = iM5.RunIndexService();
        System.out.println("Done Indexing 5" + succes);

        Thread.sleep(1000);
        String Query = "I hate hate hate hate people people";
        QueryMaster qM = new QueryMaster(Query);
        succes = qM.RunQueryService();
        HashMap<String,Integer> docIDs = qM.getQueryResult();
        System.out.println("Done Searching first time " + succes);

        Thread.sleep(1000);
        QueryMaster qM2 = new QueryMaster(Query);
        succes = qM2.RunQueryService();
        docIDs = qM2.getQueryResult();
        System.out.println("Done Searching second time " + succes);

        Thread.sleep(1000);
        QueryMaster qM3 = new QueryMaster(Query);
        succes = qM3.RunQueryService();
        docIDs = qM3.getQueryResult();
        System.out.println("Done Searching third time " + succes);

        Thread.sleep(1000);
        QueryMaster qM4 = new QueryMaster(Query);
        succes = qM2.RunQueryService();
        docIDs = qM2.getQueryResult();
        System.out.println("Done Searching fourth time " + succes);


    }
}
