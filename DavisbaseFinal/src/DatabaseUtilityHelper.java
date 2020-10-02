import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
//a utility helper class to handle input and output for a database with errors
public class DatabaseUtilityHelper {

    private static DatabaseUtilityHelper DatabaseUtilityHelper = null;

    public static DatabaseUtilityHelper getDatabaseUtilityHelper() {
        if(DatabaseUtilityHelper == null) {
            return new DatabaseUtilityHelper();
        }
        return DatabaseUtilityHelper;
    }

    private IO_Handling manager;

    private DatabaseUtilityHelper() {
        manager = new IO_Handling();
    }

    public boolean checkIfDatabaseExists(String databaseName) {

        if (databaseName == null || databaseName.length() == 0) {
            QueryHandling.commandUnrecognised("", QueryHandling.USE_HELP_MESSAGE);
            return false;
        }

        return new IO_Handling().checkDbExists(databaseName);
    }

    public boolean checkIfTableExists(String databaseName, String tableName) {
        if (tableName == null || databaseName == null || tableName.length() == 0 || databaseName.length() == 0) {
            QueryHandling.commandUnrecognised("", QueryHandling.USE_HELP_MESSAGE);
            return false;
        }

        return new IO_Handling().checkTblExists(databaseName, tableName);
    }

    public List<String> getAllTableColumns(String databaseName, String tableName) throws InternalException {
        List<String> columnNames = new ArrayList<>();
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new Text_DT(databaseName)));
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (DataRecord record : records) {
            Object object = record.getValuesOfColumns().get(DatabaseUtility.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnNames.add(((DataType) object).getStringValue());
        }

        return columnNames;
    }

    public boolean checkForNullConstraint(String databaseName, String tableName, HashMap<String, Integer> columnMap) throws InternalException {

        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new Text_DT(databaseName)));
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (DataRecord record : records) {
            Object nullValueObject = record.getValuesOfColumns().get(DatabaseUtility.COLUMNS_TABLE_SCHEMA_IS_NULLABLE);
            Object object = record.getValuesOfColumns().get(DatabaseUtility.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);

            String isNullStr = ((DataType) nullValueObject).getStringValue().toUpperCase();
            boolean isNullable = isNullStr.equals("YES");

            if (!columnMap.containsKey(((DataType) object).getStringValue()) && !isNullable) {
                Errors.printMessage("ERROR(100N): Field '" + ((DataType) object).getStringValue() + "' cannot be NULL");
                return false;
            }

        }

        return true;
    }

    public HashMap<String, Integer> getTableColumnDataTypes(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new Text_DT(databaseName)));
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        HashMap<String, Integer> columDataTypeMapping = new HashMap<>();

        for (DataRecord record : records) {
            Object object = record.getValuesOfColumns().get(DatabaseUtility.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            Object dataTypeObject = record.getValuesOfColumns().get(DatabaseUtility.COLUMNS_TABLE_SCHEMA_DATA_TYPE);

            String columnName = ((DataType) object).getStringValue();
            int columnDataType = Errors.stringToDataType(((DataType) dataTypeObject).getStringValue());
            columDataTypeMapping.put(columnName.toLowerCase(), columnDataType);
        }

        return columDataTypeMapping;
    }

    public String getPrimaryKeyOfTable(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new Text_DT(databaseName)));
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_COLUMN_KEY, InternalCondition.EQUALS, new Text_DT(DatabaseUtility.PRIMARY_KEY_IDENTIFIER)));

        List<DataRecord> records = manager.findRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, conditions, true);
        String columnName = "";
        if(records.size() > 0) {
            DataRecord record = records.get(0);
            Object object = record.getValuesOfColumns().get(DatabaseUtility.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnName = ((DataType) object).getStringValue();
        }

        return columnName;
    }

    public int getTableRecords(String databaseName, String tableName) throws InternalException {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.TABLES_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new Text_DT(databaseName)));
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, conditions, true);
        int recordCount = 0;

        if(records.size() > 0) {
            DataRecord record = records.get(0);
            Object object = record.getValuesOfColumns().get(DatabaseUtility.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            recordCount = Integer.valueOf(((DataType) object).getStringValue());
        }

        return recordCount;
    }

    public boolean checkIfPrimaryKeyExists(String databaseName, String tableName, int value) throws InternalException {
        IO_Handling manager = new IO_Handling();
        InternalCondition condition = InternalCondition.CreateCondition(0, InternalCondition.EQUALS, new Int_DT(value));

        List<DataRecord> records = manager.findRecord(databaseName, tableName, condition, false);
        return records.size() > 0;
    }
}
