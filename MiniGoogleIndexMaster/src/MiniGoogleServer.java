import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by Ahmed on 12/7/2016.
 */
public class MiniGoogleServer
{
    public static void main(String args[]) throws Exception
    {
        String Query = "I hate hate hate hate people people";
        QueryMaster qM = new QueryMaster(Query);
        qM.RunQueryService();
        HashMap<String,Integer> docIDs = qM.getQueryResult();
        System.out.println("Done");
    }
}
