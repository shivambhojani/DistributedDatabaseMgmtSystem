package Logger;

import Configs.StaticData;
import FileManager.DirectoryCreator;
import FileManager.FileCreator;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Objects;


public class LogGenerator {

    public void eventLog(String log) throws IOException {
        eventlogFileChecker();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String time = simpleDateFormat.format(timestamp);

        String eventLog = log + "|" + time;
        String path = StaticData.logPath + "/" + StaticData.eventLogsFileName;
        FileWriter file = new FileWriter(path, true);
        file.write(eventLog);
        file.write("\n");
        file.flush();
        file.close();
    }

    public void generalLog(String log) throws IOException {
        generallogFileChecker();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String time = simpleDateFormat.format(timestamp);

        String general = log + "|" + time;
        String path = StaticData.logPath + "/" + StaticData.generalLogsFileName;
        FileWriter file = new FileWriter(path, true);
        file.write(general);
        file.write("\n");
        file.flush();
        file.close();
    }

    public void logQuery(String database, String query, Boolean isValid,  String userId, String queryType, String queryTime) throws IOException {

       // dbFolderChecker(database);
        querylogFileChecker(database);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        String path = StaticData.logPath + "/" + StaticData.queryLogsFileName;
        FileWriter file = new FileWriter(path, true);

        String time = simpleDateFormat.format(timestamp);

        String queryLog = isValid + StaticData.delimiter +  queryType + StaticData.delimiter + userId + StaticData.delimiter + query + StaticData.delimiter
                + time + StaticData.delimiter + queryTime;

        file.write(queryLog);
        file.write("\n");
        file.flush();
        file.close();
    }

    public void addToLoginHistory(String userID, String logType) throws IOException {
        generallogFileChecker();
        String path = StaticData.logPath  + "/" + StaticData.generalLogsFileName;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        if (Objects.equals(logType, StaticData.login)){
            FileWriter file = new FileWriter(path, true);
            String log = userID + " logged in at " + timestamp;
            file.write(log);
            file.write("\n");
            file.flush();
            file.close();
        }
        else if (Objects.equals(logType, StaticData.logout)){
            FileWriter file = new FileWriter(path, true);
            String log = userID + " logged out at " + timestamp;
            file.write(log);
            file.write("\n");
            file.flush();
            file.close();
        }
        else {
            System.out.println("Unable to generate log for the login/logout");
        }
    }

    public void querylogFileChecker(String database) throws IOException {

        FileCreator fileCreator = new FileCreator();
        fileCreator.checkOrCreateFile(StaticData.queryLogType);

    }

    public void generallogFileChecker() throws IOException {

        FileCreator fileCreator = new FileCreator();
        fileCreator.checkOrCreateFile(StaticData.generalLogType);

    }


    public void eventlogFileChecker() throws IOException {

        FileCreator fileCreator = new FileCreator();
        fileCreator.checkOrCreateFile(StaticData.eventLogType);

    }

}

