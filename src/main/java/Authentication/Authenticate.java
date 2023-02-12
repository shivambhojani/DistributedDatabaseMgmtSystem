package Authentication;

import Configs.StaticData;
import CreateSession.SessionCreator;
import Distribution.Client;
import Logger.LogGenerator;

import java.io.IOException;
import java.util.Scanner;

public class Authenticate {
    private String loggeduserID = "";
    private Client client;

    public Authenticate(Client client) {
        this.client = client;
    }

    public void init() throws IOException, InterruptedException {
        System.out.println("1. Register");
        System.out.println("2. Login");
        System.out.println("3. Exit");
        System.out.print("Enter Your Choice: ");

        Scanner sc = new Scanner(System.in);
        String userInput = sc.nextLine();

        switch (userInput) {
            case "1":
                register();
                break;
            case "2":
                login();
                break;
            case "3":
                System.out.println("Bye!!!");
                System.exit(0);
                break;
            default:
                System.out.println("Incorrect option, Try Again");
                init();
                break;
        }
    }

    private void register() throws IOException, InterruptedException {
        Scanner sc = new Scanner(System.in);
        User user = new User();

        boolean isUserIdCorrect = false;
        while (!isUserIdCorrect) {
            System.out.println("");
            System.out.print("Enter UserId: ");
            String userId = sc.nextLine();
            if (userId.length() < 1) {
                System.out.println("userId cannot be empty.");
                continue;
            }
            if (user.userIdCheck(userId)) {
                System.out.println("userId already taken.");
                continue;
            }
            isUserIdCorrect = true;
            user.setUserId(userId);
        }

        boolean isPasswordCorrect = false;
        while (!isPasswordCorrect) {
            System.out.println("");
            System.out.print("Enter Password: ");
            String password = sc.nextLine();
            if (password.length() < 1) {
                System.out.println("password cannot be empty");
                continue;
            }
            isPasswordCorrect = true;
            user.setPassword(password);
        }

        boolean isSecurityCorrect = false;
        while (!isSecurityCorrect) {
            System.out.println("");
            System.out.print("Enter Security Question: ");
            String securityQuestion = sc.nextLine();
            if (securityQuestion.length() < 1) {
                System.out.println("Security Question cannot be empty");
                continue;
            }
            isSecurityCorrect = true;
            user.setSecurityQuestion(securityQuestion);
        }

        boolean isAnswerCorrect = false;
        while (!isAnswerCorrect) {
            System.out.println("");
            System.out.print("Enter Answer: ");
            String answer = sc.nextLine();
            if (answer.length() < 1) {
                System.out.println("Answer cannot be empty");
            }
            isAnswerCorrect = true;
            user.setAnswer(answer);
        }

        user.save();
        client.sendMessage(Constants.USER_PROFILE_TXT);
        init();
    }

    private void login() throws IOException, InterruptedException {
        Scanner sc = new Scanner(System.in);
        String userId = "";
        String password = "";

        boolean isUserIdCorrect = false;
        while (!isUserIdCorrect) {
            System.out.println("");
            System.out.print("Enter UserId: ");
            userId = sc.nextLine();

            if (userId.length() < 1) {
                System.out.println("userId cannot be empty.");
                continue;
            }
            isUserIdCorrect = true;
        }

        boolean isPasswordCorrect = false;
        while (!isPasswordCorrect) {
            System.out.println("");
            System.out.print("Enter Password: ");
            password = sc.nextLine();
            if (password.length() < 1) {
                System.out.println("password cannot be empty");
                continue;
            }
            isPasswordCorrect = true;
        }

        User user = new User();
        user = user.validateUserIdAndPassword(userId, password);
        if (user == null) {
            System.out.println("Incorrect UserId or Password");
            init();
            return;
        }

        boolean isAnswerCorrect = false;
        String answer = "";
        while (!isAnswerCorrect) {
            System.out.println("");
            System.out.print("Enter Answer for " + user.getSecurityQuestion() + ": ");
            answer = sc.nextLine();
            if (answer.length() < 1) {
                System.out.println("Answer cannot be empty");
            }
            isAnswerCorrect = true;
        }
        if (!user.getAnswer().equalsIgnoreCase(answer)) {
            System.out.println("Invalid Security");
            return;
        } else {
            loggeduserID = userId;
            LogGenerator logGenerator = new LogGenerator();
            logGenerator.addToLoginHistory(loggeduserID, StaticData.login);
            SessionCreator sessionCreator = new SessionCreator(loggeduserID, client);
            sessionCreator.createSession();
        }
    }
}
