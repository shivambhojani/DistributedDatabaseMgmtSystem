package Query;

import CreateSession.SessionCreator;
import Logger.GetTimer;
import Logger.LogGenerator;
import Transaction.Transaction;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalTime;
import java.util.*;
import Distribution.Client;
import static CreateSession.SessionCreator.userID;

public class QueryEngine {

    LogGenerator logGenerator = new LogGenerator();
    public static String dbName;
    private DBEngine databaseEngine;
    private ValidationEngine validator;
    private Client client2;

    public QueryEngine(Client client)
    {
        this.client2 = client;
        databaseEngine = new DBEngine(client);
        validator = new ValidationEngine(client);
    }

    /**
     * This method creates the database in the application.
     * The method validates the query for all necessary parameters
     * passed as argument and calls method from module 1 to create
     * the database
     *
     * @param query array of keywords passed in query as string type
     * @return true if database created successfully, else false
     */
    private boolean createDB(String[] query) throws InterruptedException, IOException {
        LocalTime l1 = GetTimer.getCurrentTime();
        boolean result = false;
        long executionTime = 0;
        //If query has exactly 3 keywords Ex. CREATE DATABASE sample
        if (query.length == 3) {
            //If database does not exist with same name
            // try to create db, else display error
            if (!validator.checkIfDbExists(query[2])) {
                //Call method from module 1 to create a new database
                //The method must return true if success, else false
                //If true, print success message
                //Example objModule1.createDB(database_name);
                result = databaseEngine.createDB(query[2]);
                LocalTime l2 = GetTimer.getCurrentTime();
                executionTime = GetTimer.duration(l1, l2);
                String q = "";
                for (String s : query) {
                    q = q + " " + s;
                }
                logGenerator.logQuery(query[2], q.trim(), true, userID, "Create", String.valueOf(executionTime));
                System.out.println("Database created successfully!");
            } else {
                String general = "User " + userID + " tried to created duplicate database with name " + query[2];
                logGenerator.eventLog(general);
                System.out.println("Error: Database already exist with name " + query[2]);
            }
        } else {
            String general = "User " + userID + " executed invalid query: " + query[2];
            logGenerator.eventLog(general);
            String q = "";
            for (String s : query) {
                q = q + " " + s;
            }
            logGenerator.logQuery(query[2], q.trim(), false, userID, "Create", String.valueOf(executionTime));
            System.out.println("Error: Invalid Command Parameters! Please try again.");
        }

        return result;
    }

    /**
     * This method sets the current the database in the application.
     * The method validates the query for all necessary parameters
     * passed as argument and calls method from module 1 to select
     * the database
     *
     * @param query array of keywords passed in query as string type
     * @return true if database selected successfully, else false
     */
    private boolean useDB(String[] query) throws InterruptedException, IOException {
        boolean result = false;
        LocalTime l1 = GetTimer.getCurrentTime();
        long executionTime = 0;
        //If query has exactly 2 keywords Ex. USE <database name>
        if (query.length == 2) {
            //If database exist with same name
            // execute query, else display error
            if (validator.checkIfDbExists(query[1])) {
                //Call method from module 1 to select database as existing
                //The method must return true if success, else false
                //If true, print success message
                //Example objModule1.useDB(database_name);
                dbName = query[1];
                result = true;
                LocalTime l2 = GetTimer.getCurrentTime();
                executionTime = GetTimer.duration(l1, l2);
                String q = "";
                for (String s : query) {
                    q = q + " " + s;
                }
                logGenerator.logQuery(query[1], q.trim(), true, userID, "Use", String.valueOf(executionTime));
                System.out.println("Database selected successfully!");
            } else {
                String general = "User " + userID + " executed query with invalid database: " + query[2];
                logGenerator.eventLog(general);
                System.out.println("Error: Database does not exist with name " + query[1]);
            }
        } else {
            String general = "User " + userID + " executed invalid query: " + query[2];
            logGenerator.eventLog(general);

            String q = "";
            for (String s : query) {
                q = q + " " + s;
            }
            logGenerator.logQuery(query[2], q.trim(), false, userID, "Use", String.valueOf(executionTime));
            System.out.println("Error: Invalid Command Parameters! Please try again.");
        }

        return result;
    }

    /**
     * This method creates a new table in the database application.
     * The method validates the query, checks if table is unique or not
     * and creates a new table if it does not exist.
     * It calls a method from module 1 to create a table. The required
     * method must accept a hashmap<String, String> as param containing all
     * table details. The method will create the table. Refer to this method
     * for more details on the structure of hashmap data.
     *
     * @param table_name Name of the table to be created
     * @param query      CREATE TABLE query
     * @return true if table created, else false
     */
    private boolean createTable(String table_name, String query) throws InterruptedException, IOException {
        LocalTime l1 = GetTimer.getCurrentTime();
        long executionTime = 0;
        boolean result = true;

        if (!validator.checkIfTableExists(dbName, table_name)) {
            String[] queryData = query.split(" ");
            String table_params = "";

            for (int cnt = 3; cnt < queryData.length; cnt++) {
                table_params += queryData[cnt] + " ";
            }
            StringBuilder tmp = new StringBuilder(table_params);
            tmp.setCharAt(0, ' ');
            tmp.setCharAt(tmp.length() - 2, ' ');

            table_params = tmp.toString();
            queryData = table_params.split(",");

            HashMap<String, String> tableMap = new HashMap<>();
            int total_cols = 0;
            tableMap.put("name", table_name);

            for (int cnt = 0; cnt < queryData.length; cnt++) {
                String[] colData = queryData[cnt].strip().split(" ");

                if (colData.length == 2) {
                    total_cols += 1;
                    tableMap.put("col_" + total_cols + "_name", colData[0].strip());
                    tableMap.put("col_" + total_cols + "_datatype", colData[1].strip());
                    tableMap.put("col_" + total_cols + "_is_null", "false");
                } else if (colData.length == 4 && colData[2].strip().matches("not") && colData[3].strip().matches("null")) {
                    total_cols += 1;
                    tableMap.put("col_" + total_cols + "_name", colData[0].strip());
                    tableMap.put("col_" + total_cols + "_datatype", colData[1].strip());
                    tableMap.put("col_" + total_cols + "_is_null", "true");
                } else if (colData.length == 3 && colData[0].strip().matches("primary")) {
                    boolean validPrimaryKey = false;
                    for (int colCount = 1; colCount <= total_cols; colCount++) {
                        String colName = tableMap.get("col_" + colCount + "_name");

                        if (colName.matches(colData[2].strip())) {
                            tableMap.put("primary_key", colData[2].strip());
                            colCount = total_cols + 1; //To exit for loop
                            validPrimaryKey = true;
                        }
                    }

                    if (!validPrimaryKey) {

                        System.out.println("Error! Invalid Primary Key Constraint.");
                        result = false;
                        break;
                    }
                } else if (colData.length == 6 && colData[0].strip().matches("foreign") &&
                        colData[1].strip().matches("key")) {
                    tableMap.put("foreign_key", colData[2].replace("\\(\\)", "").strip());
                    tableMap.put("foreign_key_table", colData[4].strip());
                    tableMap.put("foreign_key_reference", colData[5].strip());
                }
            }

            if (result) {
                tableMap.put("total_columns", Integer.toString(total_cols));

                System.out.println(tableMap.get("total_columns") + " " + tableMap.get("col_1_name"));


                result = databaseEngine.createTable(dbName, tableMap);
                //require a method from module 1 to be called
                //hashmap will be passed to that method to create the table
                //hashmap will contain following keys and associated values:
                //name: (name of the table as value)
                //total_columns: (no. of columns as string ex. 5 or 10 or 3)
                //col_[columnCount]_name: name of column 1 till total columns (each key has a name associated)
                //col_[columnCount]_datatype: datatype for a particular column
                //primary_key: primary_key if any
                //foreign_key: foreign key if any
                //foreign_key_table: table associated in foreign key column
                //foreign_key_reference: foreign key column name in the associated table

                LocalTime l2 = GetTimer.getCurrentTime();
                executionTime = GetTimer.duration(l1, l2);
                logGenerator.logQuery(dbName, query.trim(), true, userID, "Create", String.valueOf(executionTime));

                String general = "User " + userID + " created new table " + table_name + "in " + dbName;
                logGenerator.eventLog(general);

                System.out.println("Table created successfully!");
            }

        } else {
            String general = "User " + userID + " tried to create duplicate table ";
            logGenerator.eventLog(general);

            System.out.println("Error: Table already exists with name " + table_name);
            result = false;
        }

        return result;
    }

    private boolean insertRecord(String table_name, String query) throws InterruptedException, IOException {
        LocalTime l1 = GetTimer.getCurrentTime();
        long executionTime = 0;
        boolean result = false;

        String[] data = query.split("\\(");

        String newRecord = data[1].replace(")", " ").strip();
        String[] queryData = newRecord.split(",");

        if (validator.validateInsertQuery(dbName, query)) {
            result = databaseEngine.insertRecord(dbName, table_name, queryData);
        }

        if (result) {
            LocalTime l2 = GetTimer.getCurrentTime();
            executionTime = GetTimer.duration(l1, l2);
            logGenerator.logQuery(dbName, query.trim(), true, userID, "Insert", String.valueOf(executionTime));
            System.out.println("Query executed successfully!");
        }

        return result;
    }

    private boolean selectRecord(String table_name, String query) throws InterruptedException, IOException {
        boolean res = false;
        LocalTime l1 = GetTimer.getCurrentTime();
        long executionTime = 0;
        if (validator.validateSelectQuery(dbName, query) && query.contains("where")) {
            String[] splits = query.split("select | from | where");
//            System.out.println(splits.length);

            HashMap<String, String> criteria = new HashMap<>();
            criteria.put("name", splits[3].replaceAll("[><=]", " ").strip().split(" ")[0]);
            String val = splits[3].replaceAll("[><=]", " ").strip().split(" ")[1];
            val = val.replaceAll("\'", "").strip();
            criteria.put("value", val);

            if (splits[3].contains("=")) {
                criteria.put("criteria", "=");
            } else if (splits[3].contains(">")) {
                criteria.put("criteria", ">");
            } else if (splits[3].contains("<")) {
                criteria.put("criteria", "<");
            }

            res = databaseEngine.selectRecord(dbName, table_name, criteria);
        } else if (validator.validateSelectQuery(dbName, query)) {
            res = databaseEngine.selectAllRecords(dbName, table_name);
        }

        LocalTime l2 = GetTimer.getCurrentTime();
        executionTime = GetTimer.duration(l1,l2);
        logGenerator.logQuery(dbName, query.trim(), true, userID, "Select", String.valueOf(executionTime));
        return res;
    }

    private boolean updateRecord(String table_name, String query) throws InterruptedException, IOException {
        boolean result = false;
        LocalTime l1 = GetTimer.getCurrentTime();
        long executionTime = 0;
        if (validator.validateUpdateQuery(dbName, query)) {
            String[] splits = query.split("set | where");

            HashMap<String, String> criteria = new HashMap<>();
            criteria.put("name", splits[2].replaceAll("[><=]", " ").strip().split(" ")[0]);
            criteria.put("value", splits[2].replaceAll("[><=]", " ").strip().split(" ")[1]);

            if (splits[2].contains("=")) {
                criteria.put("criteria", "=");
            } else if (splits[2].contains(">")) {
                criteria.put("criteria", ">");
            } else if (splits[2].contains("<")) {
                criteria.put("criteria", "<");
            }

            HashMap<String, String> updateColumn = new HashMap<>();
            updateColumn.put("name", splits[1].replaceAll("[=]", " ").strip().split(" ")[0]);
            updateColumn.put("value", splits[1].replaceAll("[=]", " ").strip().split(" ")[1]);

            result = databaseEngine.updateRecord(dbName, table_name, criteria, updateColumn);
        }
        LocalTime l2 = GetTimer.getCurrentTime();
        executionTime = GetTimer.duration(l1,l2);
        logGenerator.logQuery(dbName, query.trim(), true, userID, "Update", String.valueOf(executionTime));
        return result;
    }

    private boolean deleteRecord(String table_name, String query) throws InterruptedException, IOException{
        boolean res = true;
        LocalTime l1 = GetTimer.getCurrentTime();
        long executionTime = 0;
        if (validator.validateDeleteQuery(dbName, query)) {
            String[] splits = query.split("from | where");

            HashMap<String, String> criteria = new HashMap<>();
            criteria.put("name", splits[2].replaceAll("[><=]", " ").strip().split(" ")[0]);
            criteria.put("value", splits[2].replaceAll("[><=]", " ").strip().split(" ")[1]);

            if (splits[2].contains("=")) {
                criteria.put("criteria", "=");
            } else if (splits[2].contains(">")) {
                criteria.put("criteria", ">");
            } else if (splits[2].contains("<")) {
                criteria.put("criteria", "<");
            }

            res = databaseEngine.deleteRecord(dbName, table_name, criteria);
        }
        LocalTime l2 = GetTimer.getCurrentTime();
        executionTime = GetTimer.duration(l1,l2);
        logGenerator.logQuery(dbName, query.trim(), true, userID, "Delete", String.valueOf(executionTime));
        return res;
    }

    public boolean execute(String query) throws IOException, InterruptedException {
        boolean result = false;

        query = query.toLowerCase().replaceAll(";", "").strip();

        String[] queryData = query.split(" ");

        switch (queryData[0]) {

            case "create":

                if (queryData[1].matches("database")) {
                    result = createDB(queryData);
                } else if (queryData[1].matches("table")) {
                    result = createTable(queryData[2], query);
                } else {
                    System.out.println("Invalid Query! Please try again.");
                }
                break;

            case "use":
                result = useDB(queryData);
                break;

            case "insert":
                result = insertRecord(queryData[2], query);
                break;

            case "select":
                queryData = query.split("from | where");
                result = selectRecord(queryData[1].strip(), query);
                break;

            case "update":
                result = updateRecord(queryData[1], query);
                break;

            case "delete":
                result = deleteRecord(queryData[2], query);
                break;

            case "start":
                if (queryData[1].equalsIgnoreCase("transaction"))
                {

                    String event = "User " + userID + " started transaction: " + query;
                    logGenerator.eventLog(event);
                    Transaction transaction = new Transaction(client2);
                    transaction.init();
                }
                break;
            default:
                String event = "User " + userID + " executed invalid query: " + query;
                logGenerator.eventLog(event);
                System.out.println("Invalid Query! Please try again.");
                break;
        }

        return result;
    }

}
