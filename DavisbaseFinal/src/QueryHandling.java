import java.util.ArrayList;
//a class and interface to handle different commands in SQL given by the user
interface QueryInterface {
    Result executeAQuery();
    boolean validateAQuery();
}

public class QueryHandling {

    static final String SELECT_COMMAND = "SELECT";
    static final String DROP_TABLE_COMMAND = "DROP TABLE";
    static final String DROP_DATABASE_COMMAND = "DROP DATABASE";
    static final String HELP_COMMAND = "HELP";
    static final String VERSION_COMMAND = "VERSION";
    static final String EXIT_COMMAND = "EXIT";
    static final String SHOW_TABLES_COMMAND = "SHOW TABLES";
    static final String SHOW_DATABASES_COMMAND = "SHOW DATABASES";
    static final String INSERT_COMMAND = "INSERT INTO";
    static final String CREATE_TABLE_COMMAND = "CREATE TABLE";
    static final String CREATE_DATABASE_COMMAND = "CREATE DATABASE";
    static final String USE_DATABASE_COMMAND = "USE";
    private static final String NO_DATABASE_SELECTED_MESSAGE = "No database selected";
    public static final String USE_HELP_MESSAGE = "\nType 'help;' to display supported commands.";

    public static String ActiveDatabaseName = "";

    private static String getVersion() {
        return DatabaseDefinedConstants.VERSION;
    }

    private static String getCopyright() {
        return DatabaseDefinedConstants.COPYRIGHT;
    }

    public static String line(String s, int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

    static QueryInterface ShowTableListQueryHandler() {
        if(QueryHandling.ActiveDatabaseName.equals("")){
            System.out.println(QueryHandling.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new ShowTbl(QueryHandling.ActiveDatabaseName);
    }

    static QueryInterface DropTblHandler(String tableName) {
        if(QueryHandling.ActiveDatabaseName.equals("")){
            System.out.println(QueryHandling.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        return new DropTbl(QueryHandling.ActiveDatabaseName, tableName);
    }

    public static void commandUnrecognised(String userCommand, String message) {
        System.out.println("ERROR(100) Unrecognised Command " + userCommand);
        System.out.println("Message : " + message);
    }

    static QueryInterface SelectQHandler(String[] attributes, String tableName, String conditionString) {
        if(QueryHandling.ActiveDatabaseName.equals("")){
            System.out.println(QueryHandling.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        boolean isSelectAll = false;
        SelectQ query;
        ArrayList<String> columns = new ArrayList<>();
        for(String attribute : attributes){
            columns.add(attribute.trim());
        }

        if(columns.size() == 1 && columns.get(0).equals("*")) {
            isSelectAll = true;
            columns = null;
        }

        if(conditionString.equals("")){
            query = new SelectQ(QueryHandling.ActiveDatabaseName, tableName, columns, null, isSelectAll);
            return query;
        }

        CheckCondition condition = CheckCondition.conditionCreate(conditionString);
        if(condition == null) return null;

        ArrayList<CheckCondition> conditionList = new ArrayList<>();
        conditionList.add(condition);
        query = new SelectQ(QueryHandling.ActiveDatabaseName, tableName, columns, conditionList, isSelectAll);
        return query;
    }

    public static void ShowVersionQueryHandler() {
        System.out.println("DavisBase Version " + getVersion());
        System.out.println(getCopyright());
    }

    static void HelpQueryHandler() {
        System.out.println(line("*",80));
        System.out.println("SUPPORTED COMMANDS");
        System.out.println("All commands below are case insensitive");
        System.out.println();
        System.out.println("\tUSE database_name;                        \t Changes current database.");
        System.out.println("\tCREATE DATABASE database_name;                   Creates an empty database.");
        System.out.println("\tSHOW DATABASES;                                  Displays all databases.");
        System.out.println("\tDROP DATABASE database_name;                     Deletes a database.");
        System.out.println("\tSHOW TABLES;                                     Displays all tables in current database.");
        System.out.println("\tCREATE TABLE table_name (                        Creates a table in current database.");
        System.out.println("\t\t<column_name> <datatype> [PRIMARY KEY | NOT NULL]");
        System.out.println("\t\t...);");
        System.out.println("\tDROP TABLE table_name;                           Deletes a table data and its schema.");
        System.out.println("\tSELECT <column_list> FROM table_name             Display records whose rowid is <id>.");
        System.out.println("\t\t[WHERE rowid = <value>];");
        System.out.println("\tINSERT INTO table_name                           Inserts a record into the table.");
        System.out.println("\t\t[(<column1>, ...)] VALUES (<value1>, <value2>, ...);");
        System.out.println("\t\t[WHERE condition];");
        System.out.println("\tHELP;                                            Displays help information");
        System.out.println("\tEXIT;                                            Exits the program");
        System.out.println();
        System.out.println();
        System.out.println(line("*",80));
    }

    static QueryInterface InsertQHandler(String tableName, String columnsString, String valuesList) {
        if(QueryHandling.ActiveDatabaseName.equals("")){
            System.out.println(QueryHandling.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        QueryInterface query = null;
        ArrayList<String> columns = null;
        ArrayList<Literal> values = new ArrayList<>();

        if(!columnsString.equals("")) {
            columns = new ArrayList<>();
            String[] columnList = columnsString.split(",");
            for(String column : columnList){
                columns.add(column.trim());
            }
        }

        for(String value : valuesList.split(",")){
            Literal literal = Literal.CreateLiteral(value.trim());
            if(literal == null) return null;
            values.add(literal);
        }

        if(columns != null && columns.size() != values.size()){
            QueryHandling.commandUnrecognised("", "Number of columns and values don't match");
            return null;
        }

        query = new InsertQ(QueryHandling.ActiveDatabaseName, tableName, columns, values);
        return query;
    }

    static QueryInterface DeleteQHandler(String tableName, String conditionString) {
        if(QueryHandling.ActiveDatabaseName.equals("")){
            System.out.println(QueryHandling.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        QueryInterface query;

        if(conditionString.equals("")){
            query = new DeleteQ(QueryHandling.ActiveDatabaseName, tableName, null);
            return query;
        }

        CheckCondition condition = CheckCondition.conditionCreate(conditionString);
        if(condition == null) return null;

        ArrayList<CheckCondition> conditions = new ArrayList<>();
        conditions.add(condition);

        query = new DeleteQ(QueryHandling.ActiveDatabaseName, tableName, conditions);
        return query;
    }

    static QueryInterface CreateTblHandler(String tableName, String columnsPart) {
        if(QueryHandling.ActiveDatabaseName.equals("")){
            System.out.println(QueryHandling.NO_DATABASE_SELECTED_MESSAGE);
            return null;
        }

        QueryInterface query;
        boolean hasPrimaryKey = false;
        ArrayList<ColumnTypeForTable> columns = new ArrayList<>();
        String[] columnsList = columnsPart.split(",");

        for(String columnEntry : columnsList){
            ColumnTypeForTable column = ColumnTypeForTable.createAColumn(columnEntry.trim());
            if(column == null) return null;
            columns.add(column);
        }

        for (int i = 0; i < columnsList.length; i++) {
            if (columnsList[i].toLowerCase().endsWith("primary key")) {
                if (i == 0) {
                    if (columns.get(i).type == Enum_DT.INT) {
                        hasPrimaryKey = true;
                    } else {
                        QueryHandling.commandUnrecognised(columnsList[i], "PRIMARY KEY has to have INT datatype");
                        return null;
                    }
                }
                else {
                    QueryHandling.commandUnrecognised(columnsList[i], "Only first column should be PRIMARY KEY and has to have INT datatype.");
                    return null;
                }

            }
        }

        query = new CreateTbl(QueryHandling.ActiveDatabaseName, tableName, columns, hasPrimaryKey);
        return query;
    }

    static QueryInterface DropDbHandler(String databaseName) {
        return new DropDb(databaseName);
    }

    static QueryInterface ShowDatabasesQueryHandler() {
        return new ShowDb();
    }

    static QueryInterface UseDatabaseQueryHandler(String databaseName) {
        return new UseDb(databaseName);
    }

    static QueryInterface CreateDbHandler(String databaseName) {
        return new CreateDb(databaseName);
    }

    public static void ExecuteQuery(QueryInterface query) {
        if(query!= null && query.validateAQuery()){
            Result result = query.executeAQuery();
            if(result != null){
                result.Display();
            }
        }
    }
}

class QueryParser {

    public static boolean isExit = false;

    public static void parseCommand(String userCommand) {
        if(userCommand.toLowerCase().equals(QueryHandling.SHOW_TABLES_COMMAND.toLowerCase())){
            QueryInterface query = QueryHandling.ShowTableListQueryHandler();
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(QueryHandling.SHOW_DATABASES_COMMAND.toLowerCase())){
            QueryInterface query = QueryHandling.ShowDatabasesQueryHandler();
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().equals(QueryHandling.HELP_COMMAND.toLowerCase())){
            QueryHandling.HelpQueryHandler();
        }
        else if(userCommand.toLowerCase().equals(QueryHandling.VERSION_COMMAND.toLowerCase())){
            QueryHandling.ShowVersionQueryHandler();
        }
        else if(userCommand.toLowerCase().equals(QueryHandling.EXIT_COMMAND.toLowerCase())){

            System.out.println("Exiting Database...");
            isExit = true;
        }
        else if(userCommand.toLowerCase().startsWith(QueryHandling.USE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryHandling.USE_DATABASE_COMMAND)){
                QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(QueryHandling.USE_DATABASE_COMMAND.length());
            QueryInterface query = QueryHandling.UseDatabaseQueryHandler(databaseName.trim());
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryHandling.DROP_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryHandling.DROP_TABLE_COMMAND)){
                QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
                return;
            }

            String tableName = userCommand.substring(QueryHandling.DROP_TABLE_COMMAND.length());
            QueryInterface query = QueryHandling.DropTblHandler(tableName.trim());
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryHandling.DROP_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryHandling.DROP_DATABASE_COMMAND)){
                QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(QueryHandling.DROP_DATABASE_COMMAND.length());
            QueryInterface query = QueryHandling.DropDbHandler(databaseName.trim());
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryHandling.SELECT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryHandling.SELECT_COMMAND)){
                QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
                return;
            }

            int index = userCommand.toLowerCase().indexOf("from");
            if(index == -1) {
                QueryHandling.commandUnrecognised(userCommand, "Expected FROM keyword");
                return;
            }

            String attributeList = userCommand.substring(QueryHandling.SELECT_COMMAND.length(), index).trim();
            String restUserQuery = userCommand.substring(index + "from".length());

            index = restUserQuery.toLowerCase().indexOf("where");
            if(index == -1) {
                String tableName = restUserQuery.trim();
                QueryInterface query = QueryHandling.SelectQHandler(attributeList.split(","), tableName, "");
                QueryHandling.ExecuteQuery(query);
                return;
            }

            String tableName = restUserQuery.substring(0, index);
            String conditions = restUserQuery.substring(index + "where".length());
            QueryInterface query = QueryHandling.SelectQHandler(attributeList.split(","), tableName.trim(), conditions);
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryHandling.INSERT_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryHandling.INSERT_COMMAND)){
                QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
                return;
            }

            String tableName = "";
            String columns = "";

            int valuesIndex = userCommand.toLowerCase().indexOf("values");
            if(valuesIndex == -1) {
                QueryHandling.commandUnrecognised(userCommand, "Expected VALUES keyword");
                return;
            }

            String columnOptions = userCommand.toLowerCase().substring(0, valuesIndex);
            int openBracketIndex = columnOptions.indexOf("(");

            if(openBracketIndex != -1) {
                tableName = userCommand.substring(QueryHandling.INSERT_COMMAND.length(), openBracketIndex).trim();
                int closeBracketIndex = userCommand.indexOf(")");
                if(closeBracketIndex == -1) {
                    QueryHandling.commandUnrecognised(userCommand, "Expected ')'");
                    return;
                }

                columns = userCommand.substring(openBracketIndex + 1, closeBracketIndex).trim();
            }

            if(tableName.equals("")) {
                tableName = userCommand.substring(QueryHandling.INSERT_COMMAND.length(), valuesIndex).trim();
            }

            String valuesList = userCommand.substring(valuesIndex + "values".length()).trim();
            if(!valuesList.startsWith("(")){
                QueryHandling.commandUnrecognised(userCommand, "Expected '('");
                return;
            }

            if(!valuesList.endsWith(")")){
                QueryHandling.commandUnrecognised(userCommand, "Expected ')'");
                return;
            }

            valuesList = valuesList.substring(1, valuesList.length()-1);
            QueryInterface query = QueryHandling.InsertQHandler(tableName, columns, valuesList);
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryHandling.CREATE_DATABASE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryHandling.CREATE_DATABASE_COMMAND)){
                QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
                return;
            }

            String databaseName = userCommand.substring(QueryHandling.CREATE_DATABASE_COMMAND.length());
            QueryInterface query = QueryHandling.CreateDbHandler(databaseName.trim());
            QueryHandling.ExecuteQuery(query);
        }
        else if(userCommand.toLowerCase().startsWith(QueryHandling.CREATE_TABLE_COMMAND.toLowerCase())){
            if(!PartsEqual(userCommand, QueryHandling.CREATE_TABLE_COMMAND)){
                QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
                return;
            }

            int openBracketIndex = userCommand.toLowerCase().indexOf("(");
            if(openBracketIndex == -1) {
                QueryHandling.commandUnrecognised(userCommand, "Expected (");
                return;
            }

            if(!userCommand.endsWith(")")){
                QueryHandling.commandUnrecognised(userCommand, "Missing )");
                return;
            }

            String tableName = userCommand.substring(QueryHandling.CREATE_TABLE_COMMAND.length(), openBracketIndex).trim();
            String columnsPart = userCommand.substring(openBracketIndex + 1, userCommand.length()-1);
            QueryInterface query = QueryHandling.CreateTblHandler(tableName, columnsPart);
            QueryHandling.ExecuteQuery(query);
        }
        else{
            QueryHandling.commandUnrecognised(userCommand, QueryHandling.USE_HELP_MESSAGE);
        }
    }

    private static boolean PartsEqual(String userCommand, String expectedCommand) {
        String[] userParts = userCommand.toLowerCase().split(" ");
        String[] actualParts = expectedCommand.toLowerCase().split(" ");

        for(int i=0;i<actualParts.length;i++){
            if(!actualParts[i].equals(userParts[i])){
                return false;
            }
        }

        return true;
    }
}
