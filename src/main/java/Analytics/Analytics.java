package Analytics;

import Configs.StaticData;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Analytics {

    private final static String generalLogsFilePath = StaticData.logPath  + "/" + StaticData.queryLogsFileName;
    public static List<QueriesCounter> queriesCounters = new ArrayList<>();
    public static List<String> logs = new ArrayList<>();
    public static List<UpdateQueries> updateQueries = new ArrayList<>();
    public static final String path = StaticData.AnalyticsPath + "/" + StaticData.analyticsFile;

    public static void main(String[] args) throws IOException {
        System.out.println("Analytics");
        Analytics analytics = new Analytics();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter analytics query string");
        String queryString = scanner.nextLine();

        readLogFile();
        generateUserQueries();
        generateUpdateQueries();

        if(queryString.contains("count queries")) {
            String [] entries = queryString.split(" ");
            if(!(entries.length==2)) {
                System.out.println("Invalid count query");
                return;
            }
            if(entries[1].contains(";")) {
                analytics.printCreateLogs(queriesCounters);
            }
            else{
                System.out.println("Invalid query");
            }
        }else if(queryString.contains("count update DB1;")){
            String [] entries = queryString.split(" ");
            if(!(entries.length==3)) {
                System.out.println("Invalid query");
                return;
            }
            if(entries[2].contains(";")) {
                analytics.printUpdateQueries(updateQueries);
            }
            else{
                System.out.println("Invalid query");
            }
        }
        else if (queryString.contains("count update DB2;")) {
            String[] entries = queryString.split(" ");
            if (!(entries.length == 3)) {
                System.out.println("Invalid query");
                return;
            }
            if (entries[2].contains(";")) {
                analytics.printUpdateQueries2(updateQueries);
            } else {
                System.out.println("Invalid query");
            }
        }
        return;
    }

    public static void printUpdateQueries2(List<UpdateQueries> updateQueries) throws IOException {
        if (updateQueries.size() == 0) {
            System.out.println("No update queries performed on db2");
            return;
        }
        FileWriter file = new FileWriter(path, true);
        for (UpdateQueries update : updateQueries) {
            file.write("Total " + update.getNoOfQueries() + " Update operations are performed on " + update.getTableName() +"\n");
            System.out.printf("Total " + update.getNoOfQueries() + "Update operations are performed on " + update.getTableName() + "\n");
        }
        file.flush();
        file.close();
    }

    public static void printUpdateQueries(List<UpdateQueries> updateQueries) throws IOException {
        if (updateQueries.size() == 0) {
            System.out.println("No update queries performed on db1");
            return;
        }
        FileWriter file = new FileWriter(path, true);
        for (UpdateQueries update : updateQueries) {
            file.write("========================Update Queries===================================");
            file.write("Total " + update.getNoOfQueries() + " Update operations are performed on " + update.getTableName() +"\n");
            System.out.printf("Total " + update.getNoOfQueries() + "Update operations are performed on " + update.getTableName() + "\n");
        }
        file.flush();
        file.close();
    }

    public static void generateUpdateQueries() {
        if (logs.size() == 0) {
            System.out.println("No operations performed");
            return;
        }
        for (String log : logs) {
            if (log.contains("|")) {
                List<String> logValues = List.of(log.split("\\|"));
                if (logValues.contains("Update")) {
                    String dbQuery = logValues.get(3);
                    String tableName = dbQuery.split(" ")[1];
                    if (log.contains(tableName)) {
                        if (updateQueries.size() == 0) {
                            UpdateQueries up = new UpdateQueries();
                            up.setTableName(tableName);
                            up.setNoOfQueries(1);
                            updateQueries.add(up);
                        } else {
                            if (updateQueries.size() > 0) {
                                boolean tableInUse = false;
                                for (UpdateQueries query : updateQueries) {
                                    if (query.getTableName().equalsIgnoreCase(tableName)) {
                                        tableInUse = true;
                                    }
                                }
                                if (tableInUse == true) {
                                    for (UpdateQueries query : updateQueries) {
                                        if (query.getTableName().equalsIgnoreCase(tableName)) {
                                            int noQueries = query.getNoOfQueries();
                                            query.setNoOfQueries(noQueries + 1);
                                        }
                                    }
                                } else {
                                    UpdateQueries up = new UpdateQueries();
                                    up.setTableName(tableName);
                                    up.setNoOfQueries(1);
                                    updateQueries.add(up);
                                }
                            }
                        }
                    }
                }
            }else {
                System.out.println("No queries performed");
            }
        }
    }

    public static void printCreateLogs(List<QueriesCounter> counters) throws IOException {
        if (queriesCounters.size() == 0) {
            System.out.println("No operations on the DB");
            return;
        }
        FileWriter file = new FileWriter(path, true);
        for (QueriesCounter count : counters) {
            if (count.getUserName().equalsIgnoreCase("abc") || count.getUserName().equalsIgnoreCase("chirag") || count.getUserName().equalsIgnoreCase("dbuser")) {
                file.write("User " + count.getUserName() + " has executed " + count.getNumberOfQueries() + " for DB2 running on Virtual Machine 2\n");
                System.out.printf("User " + count.getUserName() + " has executed " + count.getNumberOfQueries() + " for DB2 running on Virtual Machine 2\n");
            } else {
                file.write("User " + count.getUserName() + " has executed " + count.getNumberOfQueries() + " for DB1 running on Virtual Machine 1\n");
                System.out.printf("User " + count.getUserName() + " has executed " + count.getNumberOfQueries() + " for DB1 running on Virtual Machine 1\n");
            }
        }
        file.flush();
        file.close();
    }

    public static void generateUserQueries() {
        if (logs.size() == 0) {
            System.out.println("No operations performed");
            return;
        }
        for (String log : logs) {
            if (log.contains("|")) {
                List<String> logValues = List.of(log.split("\\|"));
                String username = logValues.get(2);
                if (logValues.contains(username)) {
                    if (queriesCounters.size() == 0) {
                        QueriesCounter qc = new QueriesCounter();
                        qc.setUserName(username);
                        qc.setNumberOfQueries(1);
                        queriesCounters.add(qc);
                    } else {
                        if (queriesCounters.size() > 0) {
                            boolean userInUse = false;
                            for (QueriesCounter obj : queriesCounters) {
                                if (obj.getUserName().equalsIgnoreCase(username)) {
                                    userInUse = true;
                                }
                            }
                            if (userInUse == true) {
                                for (QueriesCounter obj : queriesCounters) {
                                    if (obj.getUserName().equalsIgnoreCase(username)) {
                                        int noQueries = obj.getNumberOfQueries();
                                        obj.setNumberOfQueries(noQueries + 1);
                                    }
                                }
                            } else {
                                QueriesCounter qc = new QueriesCounter();
                                qc.setUserName(username);
                                qc.setNumberOfQueries(1);
                                queriesCounters.add(qc);
                            }
                        }
                    }
                }
            } else {
                System.out.println("No queries performed");
            }
        }
    }


    public static void readLogFile() throws IOException {
        boolean fileExits = Files.exists(Path.of(StaticData.logPath  + "/" + StaticData.queryLogsFileName));
        if (fileExits) {
            File generalLogs = new File(generalLogsFilePath);
            FileReader logReader = new FileReader(generalLogs);
            BufferedReader reader = new BufferedReader(logReader);
            String nextLine = "";
            while ((nextLine = reader.readLine()) != null) {
                logs.add(nextLine);
            }
        }
    }

}
