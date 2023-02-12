package FileManager;

import Configs.StaticData;

import java.io.File;


public class DirectoryCreator {

    public boolean checkOrCreateDirectory(String databaseName){

        String path = StaticData.logPath+"/" + databaseName;
        File directory = new File(path);
        if (directory.exists()==false){
            directory.mkdir();
            return true;
        }
        else {
            System.out.println("Directory already exists");
            return false;
        }

    }

}
