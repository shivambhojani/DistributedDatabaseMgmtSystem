package Query;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import Distribution.Client;

public class DBEngine {

    private Client client;

    public DBEngine(Client tClient)
    {
        client = tClient;
    };

    public boolean createDB(String dbName)
    {
        boolean res = false;
        try {
            Files.createDirectory(Path.of(dbName));
            Files.createFile(Path.of(dbName, "meta.txt"));
            Files.createFile(Path.of(dbName, "relationships.txt"));
            res = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;
    }

    private boolean updateMetadata(String dbName, HashMap<String, String> tableData)
    {
        boolean res = false;

        try {
            String filePath = Path.of(dbName + "/" + "meta.txt").toString();
            BufferedWriter fileHandle = new BufferedWriter(new FileWriter(filePath, true));

            int totalColumns = Integer.parseInt(tableData.get("total_columns"));

            for(int cnt=1; cnt <= totalColumns; cnt++)
            {
                String line = tableData.get("name") + "|" + tableData.get("col_" + cnt + "_name") + "|" +
                        tableData.get("col_" + cnt + "_datatype");
                if(tableData.get("col_" + cnt + "_is_null").matches("true"))
                {
                    line+= "| NOT NULL\n";
                }
                else
                {
                    line+= "\n";
                }

                fileHandle.write(line);
            }

            fileHandle.close();
            client.sendMessage(dbName + "/" + "meta.txt");
            res  =true;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;
    }

    private boolean updateRelationships(String dbName, HashMap<String, String> tableData)
    {
        boolean res = false;

        try {
            String filePath = Path.of(dbName + "/" + "relationships.txt").toString();
            BufferedWriter fileHandle = new BufferedWriter(new FileWriter(filePath, true));

            String line = tableData.get("name") + "|" + tableData.get("primary_key") + "\n";
            line+= tableData.get("name") + "|" + tableData.get("foreign_key") + "|" + tableData.get("foreign_key_table")
                    + "|" + tableData.get("foreign_key_reference") + "\n";

            fileHandle.write(line);
            fileHandle.close();
            client.sendMessage(dbName + "/" + "relationships.txt");

            res  =true;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;
    }

    public boolean createTable(String dbName, HashMap<String, String> tableData)
    {
        boolean res = false;

        if(updateMetadata(dbName, tableData) && updateRelationships(dbName, tableData))
        {
            try {
                String filePath = Path.of(dbName + "/" + tableData.get("name") + ".txt").toString();
                BufferedWriter fileHandle = new BufferedWriter(new FileWriter(filePath, true));

                int totalColumns = Integer.parseInt(tableData.get("total_columns"));
                String line = "";
                for(int cnt=1; cnt <= totalColumns; cnt++)
                {
                    line += tableData.get("col_" + cnt + "_name");
                    if(cnt < totalColumns)
                    {
                        line+= "|";
                    }

                }
                line+= "\n";
                fileHandle.write(line);
                fileHandle.close();
                client.sendMessage(dbName + "/" + tableData.get("name") + ".txt");
                res= true;

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else
        {
            System.out.println("Error! Unable to access metadata.");
        }

        return res;

    }

    public List<HashMap<String, String>> getTableMetadata(String dbName, String tableName)
    {
        List<HashMap<String, String>> metadata = new ArrayList<>();

        try {
            String filePath = Path.of(dbName + "/" + "meta.txt").toString();
            BufferedReader fileHandle = new BufferedReader(new FileReader(filePath));

            String line;
            // Condition holds true till
            // there is character in a string
            while ((line = fileHandle.readLine()) != null)
            {
                if(line.strip().split("\\|")[0].matches(tableName))
                {
                    String [] data = line.strip().split("\\|");

                    if(data[2].toLowerCase().matches("varchar"))
                    {
                        HashMap<String, String> map = new HashMap<>();
                        map.put("name", data[1]);
                        map.put("type", data[2]);
                        map.put("size", data[3]);
                        metadata.add(map);
                    }
                    else
                    {
                        HashMap<String, String> map = new HashMap<>();
                        map.put("name", data[1]);
                        map.put("type", data[2]);
                        metadata.add(map);
                    }
                }

            }

            fileHandle.close();
            client.sendMessage(dbName + "/" + "meta.txt");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return metadata;
    }

    public List<String> getTables(String dbName)
    {
        List<String> tableList = new ArrayList<>();

        try {
            String filePath = Path.of(dbName + "/" + "meta.txt").toString();
            BufferedReader fileHandle = new BufferedReader(new FileReader(filePath));

            String line;
            // Condition holds true till
            // there is character in a string
            while ((line = fileHandle.readLine()) != null)
            {
                String [] data = line.strip().split("\\|");

                if(!tableList.contains(data[0]))
                {
                    tableList.add(data[0]);
                }

            }

            fileHandle.close();
            client.sendMessage(dbName + "/" + "meta.txt");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tableList;
    }

    public List<String> getTableHeadings(String dbName, String tableName)
    {
        List<String> tableHeader = new ArrayList<>();

        try {
            String filePath = Path.of(dbName + "/" + tableName + ".txt").toString();
            BufferedReader fileHandle = new BufferedReader(new FileReader(filePath));

            String line = fileHandle.readLine();
            String [] data = line.strip().split("\\|");

            for(int cnt=0; cnt<data.length; cnt++)
            {
                tableHeader.add(data[cnt].strip());
            }

            fileHandle.close();
            client.sendMessage(dbName + "/" + tableName + ".txt");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tableHeader;
    }

    public int getMaxRecords(String dbName, String tableName)
    {
        int len = 0;

        try {
            BufferedReader fileHandle = new BufferedReader(new FileReader(dbName + "/" + tableName + ".txt"));

            String line;
            // Condition holds true till
            // there is character in a string
            while ((line = fileHandle.readLine()) != null)
            {
                len+= 1;
            }
            fileHandle.close();
            client.sendMessage(dbName + "/" + tableName + ".txt");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        len -= 1;
        return len;
    }

    public List<String> getTableData(String dbName, String tableName)
    {
        List<String> data = new ArrayList<>();
        try {
            BufferedReader fileHandle = new BufferedReader(new FileReader(dbName + "/" + tableName + ".txt"));

            String line;
            // Condition holds true till
            // there is character in a string
            while ((line = fileHandle.readLine()) != null)
            {
                data.add(line.strip());
            }

            fileHandle.close();
            client.sendMessage(dbName + "/" + tableName + ".txt");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    public boolean insertRecord(String dbName, String tableName, String [] values)
    {
        boolean res = false;
        String record = "";

        for(int cnt=0; cnt < values.length; cnt++)
        {
            record += values[cnt].replace("'","").strip();

            if(cnt < values.length - 1)
            {
                record+= "|";
            }
        }

        record += "\n";


        try {
            BufferedWriter fileHandle = new BufferedWriter(new FileWriter(dbName + "/" + tableName + ".txt", true));

            fileHandle.write(record);
            fileHandle.close();
            client.sendMessage(dbName + "/" + tableName + ".txt");
            res = true;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    private boolean overwriteFile(String dbName, String tableName, List<String> data)
    {
        boolean res = false;

        try {
            BufferedWriter fileHandle = new BufferedWriter(new FileWriter(dbName + "/" + tableName + ".txt"));

            for(int cnt=0; cnt<data.size(); cnt++)
            {
                fileHandle.write(data.get(cnt) + "\n");
            }

            fileHandle.close();
            client.sendMessage(dbName + "/" + tableName + ".txt");
            res = true;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return res;
    }

    public boolean updateRecord(String dbName, String tableName, HashMap<String, String> criteria, HashMap<String, String> column)
    {
        boolean res = true;

        List<String> tableData = getTableData(dbName, tableName);
        List<String> tableHeadings = Arrays.asList(tableData.get(0).split("\\|"));

        int criteriaIndex = tableHeadings.indexOf(criteria.get("name"));
        int updateIndex = tableHeadings.indexOf(column.get("name"));

        for(int cnt=0; cnt<tableData.size(); cnt++)
        {
            String[] values = tableData.get(cnt).split("\\|");

            if(criteria.get("criteria").matches("=") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) == 0))
            {
                values[updateIndex] = column.get("value");

                String record = "";
                for(int colCnt=0; colCnt < values.length; colCnt++)
                {
                    record += values[colCnt].replace("'","").strip();

                    if(colCnt < values.length - 1)
                    {
                        record+= "|";
                    }
                }
                tableData.set(cnt, record);
            }
            else if(criteria.get("criteria").matches(">") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) > 0))
            {
                values[updateIndex] = column.get("value");

                String record = "";
                for(int colCnt=0; colCnt < values.length; colCnt++)
                {
                    record += values[colCnt].replace("'","").strip();

                    if(colCnt < values.length - 1)
                    {
                        record+= "|";
                    }
                }
                tableData.set(cnt, record);
            }
            else if(criteria.get("criteria").matches("<") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) < 0))
            {
                values[updateIndex] = column.get("value");

                String record = "";
                for(int colCnt=0; colCnt < values.length; colCnt++)
                {
                    record += values[colCnt].replace("'","").strip();

                    if(colCnt < values.length - 1)
                    {
                        record+= "|";
                    }
                }
                tableData.set(cnt, record);
            }

        }

        overwriteFile(dbName, tableName, tableData);
        System.out.println("Record updated successfully.");

        return res;
    }

    public boolean deleteRecord(String dbName, String tableName, HashMap<String, String> criteria)
    {
        boolean res = true;

        List<String> tableData = getTableData(dbName, tableName);
        List<String> tableHeadings = Arrays.asList(tableData.get(0).split("\\|"));

        int criteriaIndex = tableHeadings.indexOf(criteria.get("name"));

        for(int cnt=1; cnt<tableData.size(); cnt++)
        {
            String[] values = tableData.get(cnt).split("\\|");

            if(criteria.get("criteria").matches("=") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) == 0))
            {
                tableData.remove(cnt);
            }
            else if(criteria.get("criteria").matches(">") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) > 0))
            {
                tableData.remove(cnt);
            }
            else if(criteria.get("criteria").matches("<") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) < 0))
            {
                tableData.remove(cnt);
            }

        }

        res = overwriteFile(dbName, tableName, tableData);
        return res;
    }

    public boolean selectRecord(String dbName, String tableName, HashMap<String, String> criteria)
    {
        boolean res = true;

        List<String> tableData = getTableData(dbName, tableName);
        List<String> tableHeadings = Arrays.asList(tableData.get(0).split("\\|"));

        int criteriaIndex = tableHeadings.indexOf(criteria.get("name"));

        System.out.println("" + tableData.get(0));

        for(int cnt=1; cnt<tableData.size(); cnt++)
        {
            String[] values = tableData.get(cnt).split("\\|");

            if(criteria.get("criteria").matches("=") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) == 0))
            {
                System.out.println(tableData.get(cnt));
            }
            else if(criteria.get("criteria").matches(">") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) > 0))
            {
                System.out.println(tableData.get(cnt));
            }
            else if(criteria.get("criteria").matches("<") &&
                    (values[criteriaIndex].compareTo(criteria.get("value")) < 0))
            {
                System.out.println(tableData.get(cnt));
            }

        }

        return res;
    }

    public boolean selectAllRecords(String dbName, String tableName)
    {
        boolean res = true;

        List<String> tableData = getTableData(dbName, tableName);

        System.out.println("\n" + tableData.get(0));

        for(int cnt=1; cnt<tableData.size(); cnt++)
        {
            System.out.println(tableData.get(cnt));
        }

        return res;
    }

}
