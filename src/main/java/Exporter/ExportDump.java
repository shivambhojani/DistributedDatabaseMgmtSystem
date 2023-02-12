package Exporter;

import Configs.StaticData;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class ExportDump {

    List<String> queryDump = new ArrayList<>();
    Scanner sc = new Scanner(System.in);

    //    public void createSQLDump(String database) {
    public void createSQLDump() throws IOException {

        List<String> getTables = new ArrayList<>();

        System.out.println("");
        System.out.print("Enter Database Name: ");
        String dbName = sc.nextLine();

        Boolean dbExists = checkDatabase(dbName);
        if (dbExists) {

            getTables = getTables(dbName);

            if (getTables.contains("meta")) {
                getTables.remove("meta");
            }

            if (getTables.contains("uml")) {
                getTables.remove("uml");
            }
            if (getTables.contains("relationships")) {
                getTables.remove("relationships");
            }

            String allCreateQueries = "";
            if (getTables.size() > 0) {
                allCreateQueries = createTablesSQL(getTables, dbName);
            }

            String allInsertQuery = "";

            //logic to get insert queries for each table
            if (getTables.size() > 0) {
                for (int i = 0; i < getTables.size(); i++) {
                   allInsertQuery =  allInsertQuery + getInsertQuery(dbName, getTables.get(i));
                }
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm");
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            String time = simpleDateFormat.format(timestamp);
            String path = StaticData.SQLDumpPath + "/" + dbName +  "dump.txt";
            FileWriter fileWriter = new FileWriter(path);
            fileWriter.write(allCreateQueries+allInsertQuery);
            fileWriter.flush();
            fileWriter.close();
            System.out.println("SQL Dump File Created at location :" + path);

        } else {
            System.out.println("Database does not exists");
        }
    }

    public String getInsertQuery(String dbName, String tableName) {
        String path = "./" + dbName + "/" + tableName + ".txt";
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);

            //insert into student (id, name, last_name) values (6, 'testcase' ,'testvalue');
            String allInsertQuery = "";
            String insert = "insert into "+ tableName;
            int i = 0;
            while (myReader.hasNextLine()) {
                String column = "";
                if (i == 0) {
                    String[] columnNames = myReader.nextLine().split("\\|");
                    column = "(";
                    for (String s : columnNames){
                        column = column + s + ",";
                    }
                    column = column.substring(0, column.length()-1);
                    column = column + ") ";
                    insert = insert + " " + column + "values";
                }
                if (i >= 1) {
                    String dataValues ="";
                    String[] data = myReader.nextLine().split("\\|");
                    dataValues = " (";
                    for (String s : data)
                    {
                        dataValues = dataValues + s + ",";
                    }
                    dataValues = dataValues.substring(0, dataValues.length()-1);
                    dataValues = dataValues + ")";
                    allInsertQuery = allInsertQuery + insert + dataValues + ";" + "\n";
                }
                i++;

            }
            return allInsertQuery;

        } catch (Exception e) {
            return null;
        }

    }

    public List<String> getTables(String dataBase) {
        String path = "./" + dataBase;
        File f = new File(path);
        String[] tables = f.list();

        List<String> tableName = new ArrayList<>();
        for (int i = 0; i < tables.length; i++) {
            tableName.add(tables[i].substring(0, tables[i].lastIndexOf('.')));
        }
        return tableName;
    }

    public String createTablesSQL(List<String> tables, String dbName) throws FileNotFoundException {

        String createQueries = "";

        for (int i = 0; i < tables.size(); i++) {
            String createTable = "create table " + tables.get(i) + "(";
            String meta = getTableMetaInfo(tables.get(i), dbName);
            createTable = createTable + meta + ");";
            createQueries = createQueries + createTable + "\n";
        }
        return createQueries;
    }

    public String getTableMetaInfo(String tableName, String dbName) throws FileNotFoundException {
        String path = "./" + dbName + "/meta.txt";
        Map<String, String> tableMeta = new HashMap<>();
        String meta = "(";
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);

            while (myReader.hasNextLine()) {
                String[] data = myReader.nextLine().split("\\|");

                if (Objects.equals(tableName, data[0])) {
                    if (data.length == 4) {
                        //create table customers(customer_id int,customer_name varchar(100));
                        // 0     1      2     3
                        //tName|name|varchar|255
                        meta = meta + data[1] + " " + data[2] + "(" + data[3] + "),";
                    } else if (data.length == 3) {
                        meta = meta + data[1] + " " + data[2] + ",";
                    }
                }
            }
            meta = meta.substring(0, meta.length() - 1);
            meta = meta + ")";
            //System.out.println(meta);
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return meta;
    }

    public boolean checkDatabase(String databseName) {
        String path = "./" + databseName;
        File f = new File(path);
        return f.exists();
    }
}
