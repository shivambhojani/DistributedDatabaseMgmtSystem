package Query;

import java.io.IOException;
import java.util.Scanner;
import Distribution.Client;

public class EngineController {

    private Client client;

    public EngineController(Client tClient)
    {
        client = tClient;
    }

    public void printMenu()
    {
        System.out.print("sql > ");
    }

    public void startEngine() throws IOException, InterruptedException {
        String userInput = "";
        QueryEngine engine = new QueryEngine(client);
        Scanner sc = new Scanner(System.in);

        while(true)
        {
            printMenu();
            userInput = sc.nextLine();
            if(userInput.toLowerCase().contains("exit"))
            {
                System.out.println("Exiting query engine!");
                break;
            }
            else
            {
                engine.execute(userInput);
            }
        }

    }

}
