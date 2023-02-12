package FileManager;

import Configs.StaticData;

import java.io.File;
import java.io.IOException;


public class FileCreator {

    public void checkOrCreateFile(String fileType) throws IOException {
        String path = "";
        if (fileType.equalsIgnoreCase(StaticData.generalLogType)) {
            path = StaticData.logPath + "/" + StaticData.generalLogsFileName;
        } else if (fileType.equalsIgnoreCase(StaticData.queryLogType)) {
            path = StaticData.logPath + "/" + StaticData.queryLogsFileName;
        } else if (fileType.equalsIgnoreCase(StaticData.eventLogType)) {
            path = StaticData.logPath + "/" + StaticData.eventLogsFileName;
        }
        File file = new File(path);
        if (file.exists()) {
            return;
        } else {
            file.createNewFile();
            System.out.println("Log file created under Directory: " + StaticData.logPath);
            return;
        }
    }

}


