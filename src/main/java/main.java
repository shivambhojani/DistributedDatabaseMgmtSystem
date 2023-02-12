import Authentication.Authenticate;
import Distribution.Client;
import Logger.GetTimer;

import java.io.IOException;
import java.net.Socket;

public class main {

    public static void main(String[] args) throws IOException, InterruptedException {

//        LogGenerator q = new LogGenerator();
//        q.logQuery("database1", "Select * from person;", true, "Shivam", "select",
//                10, 10, 50);
//
//        q.logQuery("database1", "Select * from person3;", true, "Shivam", "select",
//                10, 10, 50);
//
//        q.logQuery("database1", "Select * from person2;", true, "Shivam", "select",
//                10, 10, 50);

        Socket socket = new Socket("35.203.115.127", 1234);
        Client client = new Client(socket, "1");
        System.out.println("Server Connected");
        client.listenForMessage();
        client.sendMessage("./dontDelete.txt");


        Authenticate authenticate = new Authenticate(client);
        authenticate.init();

//        GetTimer t = new GetTimer();
//        t.getCurrentTime();


//        ExportDump e = new ExportDump();
//        e.createSQLDump();


    }

}
