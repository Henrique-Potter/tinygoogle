import com.sun.java.util.jar.pack.ConstantPool;

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
            ServerStub SS = new ServerStub();
            SS.executeService(cs);
        }

    }

    public static void main(String args[]) throws Exception
    {

        ServerSocket s = null;
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

        String Query = "I hate hate hate hate people people";
        QueryMaster qM = new QueryMaster(Query);
        qM.RunQueryService();
        HashMap<String,Integer> docIDs = qM.getQueryResult();
        System.out.println("Done");
    }
}
