package Transaction;

import CreateSession.SessionCreator;
import Distribution.Client;
import Logger.LogGenerator;
import Query.QueryEngine;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static CreateSession.SessionCreator.userID;

public class Transaction {

    private Client clientT;

    public Transaction (Client client) {
        this.clientT = client;
    }

    LogGenerator logGenerator = new LogGenerator();

    public double init() throws IOException, InterruptedException {
        System.out.println("Starting Transaction......");
        logGenerator.logQuery(QueryEngine.dbName, "start transaction", true, userID, "Start","0");
        boolean isTrue = true;
        List<String> queries = new ArrayList<String>();

        while (isTrue) {
            Scanner sc = new Scanner(System.in);
            System.out.print("Write Query: ");
            String input = sc.nextLine();
            if (input.length() > 0) {
                if (input.equalsIgnoreCase("commit;") || input.equalsIgnoreCase("commit")) {
                    logGenerator.logQuery(QueryEngine.dbName, "commit", true, userID, "commit","0");
                    isTrue = false;
                } else if (input.equalsIgnoreCase("rollback;") || input.equalsIgnoreCase("rollback")) {
                    isTrue = false;
                    logGenerator.logQuery(QueryEngine.dbName, "rollback", true, userID, "rollback","0");
                    queries.clear();
                } else {
                    queries.add(input);
                }
            }
        }

        QueryEngine queryEngine = new QueryEngine(clientT);

        double startTime = System.nanoTime();
        for (String item : queries) {
            if(item.trim().length() > 0){
                queryEngine.execute(item);
            }
        }
        double endTime = System.nanoTime();

        return endTime - startTime;
    }
}
