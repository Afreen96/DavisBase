import java.io.File;
import java.util.ArrayList;
import java.util.List;

//a utility class for the internal schema labelling and control
public class DatabaseUtility {

    public static final byte TABLES_TABLE_SCHEMA_ROWID = 0;
    public static final byte TABLES_TABLE_SCHEMA_DATABASE_NAME = 1;
    public static final byte TABLES_TABLE_SCHEMA_TABLE_NAME = 2;
    public static final byte TABLES_TABLE_SCHEMA_RECORD_COUNT = 3;
    public static final byte TABLES_TABLE_SCHEMA_COL_TBL_ST_ROWID = 4;
    public static final byte TABLES_TABLE_SCHEMA_NXT_AVL_COL_TBL_ROWID = 5;


    public static final byte COLUMNS_TABLE_SCHEMA_ROWID = 0;
    public static final byte COLUMNS_TABLE_SCHEMA_DATABASE_NAME = 1;
    public static final byte COLUMNS_TABLE_SCHEMA_TABLE_NAME = 2;
    public static final byte COLUMNS_TABLE_SCHEMA_COLUMN_NAME = 3;
    public static final byte COLUMNS_TABLE_SCHEMA_DATA_TYPE = 4;
    public static final byte COLUMNS_TABLE_SCHEMA_COLUMN_KEY = 5;
    public static final byte COLUMNS_TABLE_SCHEMA_ORDINAL_POSITION = 6;
    public static final byte COLUMNS_TABLE_SCHEMA_IS_NULLABLE = 7;

    public static final String PRIMARY_KEY_IDENTIFIER = "PRI";

    public static void initializeTheDatabase() {
        File baseDir = new File(DatabaseDefinedConstants.DEFAULT_DATA_DIRNAME);
        if(!baseDir.exists()) {
            File catalogDir = new File(DatabaseDefinedConstants.DEFAULT_DATA_DIRNAME + "/" + DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME);
            if(!catalogDir.exists()) {
                if(catalogDir.mkdirs()) {
                    new DatabaseUtility().createDatabaseCatalog();
                }
            }
        }

    }
//creating a default catalog database
    public boolean createDatabaseCatalog() {
        try {
            IO_Handling manager = new IO_Handling();
            manager.createNewTable(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            manager.createNewTable(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME + DatabaseDefinedConstants.DEFAULT_FILE_EXTENSION);
            int startingRowId = this.updateInternalTable(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, 6);
            startingRowId *= this.updateInternalTable(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, 8);
            if (startingRowId >= 0) {
                List<InternalColumn> columns = new ArrayList<>();
                columns.add(new InternalColumn("rowid", Enum_DT.INT.toString(), false, false));
                columns.add(new InternalColumn("database_name", Enum_DT.TEXT.toString(), false, false));
                columns.add(new InternalColumn("table_name", Enum_DT.TEXT.toString(), false, false));
                columns.add(new InternalColumn("record_count", Enum_DT.INT.toString(), false, false));
                columns.add(new InternalColumn("col_tbl_st_rowid", Enum_DT.INT.toString(), false, false));
                columns.add(new InternalColumn("nxt_avl_col_tbl_rowid", Enum_DT.INT.toString(), false, false));
                this.updateInternalColumns(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, 1, columns);
                columns.clear();
                columns.add(new InternalColumn("rowid", Enum_DT.INT.toString(), false, false));
                columns.add(new InternalColumn("database_name", Enum_DT.TEXT.toString(), false, false));
                columns.add(new InternalColumn("table_name", Enum_DT.TEXT.toString(), false, false));
                columns.add(new InternalColumn("column_name", Enum_DT.TEXT.toString(), false, false));
                columns.add(new InternalColumn("data_type", Enum_DT.TEXT.toString(), false, false));
                columns.add(new InternalColumn("column_key", Enum_DT.TEXT.toString(), false, false));
                columns.add(new InternalColumn("ordinal_position", Enum_DT.TINYINT.toString(), false, false));
                columns.add(new InternalColumn("is_nullable", Enum_DT.TEXT.toString(), false, false));
                this.updateInternalColumns(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, 7, columns);
            }
            return true;
        }
        catch (InternalException e) {
            Errors.printMessage(e.getMessage());
        }
        return false;
    }

//to update all the internal tables in the database system
    public int updateInternalTable(String databaseName, String tableName, int columnCount) {
        try {
        /*
         * System Tables Table Schema:
         * Column_no    Name                                    Data_type
         *      1       rowid                                   INT
         *      2       database_name                           TEXT
         *      3       table_name                              TEXT
         *      4       record_count                            INT
         *      5       col_tbl_st_rowid                        INT
         *      6       nxt_avl_col_tbl_rowid                   INT
         */
            IO_Handling manager = new IO_Handling();
            List<InternalCondition> conditions = new ArrayList<>();
            conditions.add(InternalCondition.CreateCondition(DatabaseUtility.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));
            conditions.add(InternalCondition.CreateCondition(DatabaseUtility.TABLES_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new Text_DT(databaseName)));
            List<DataRecord> result = manager.findRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, conditions, true);
            if (result != null && result.size() == 0) {
                int returnValue = 1;
                Page<DataRecord> page = manager.getLastRecordAndPage(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME);
                //Check if record exists
                DataRecord lastRecord = null;
                if (page.getPageRecords().size() > 0) {
                    lastRecord = page.getPageRecords().get(0);
                }
                DataRecord record = new DataRecord();
                if (lastRecord == null) {
                    record.setRowId(1);
                } else {
                    record.setRowId(lastRecord.getRowId() + 1);
                }
                record.getValuesOfColumns().add(new Int_DT(record.getRowId()));
                record.getValuesOfColumns().add(new Text_DT(databaseName));
                record.getValuesOfColumns().add(new Text_DT(tableName));
                record.getValuesOfColumns().add(new Int_DT(0));
                if (lastRecord == null) {
                    record.getValuesOfColumns().add(new Int_DT(1));
                    record.getValuesOfColumns().add(new Int_DT(columnCount + 1));
                } else {
                    Int_DT startingColumnIndex = (Int_DT) lastRecord.getValuesOfColumns().get(DatabaseUtility.TABLES_TABLE_SCHEMA_NXT_AVL_COL_TBL_ROWID);
                    returnValue = startingColumnIndex.getValue();
                    record.getValuesOfColumns().add(new Int_DT(returnValue));
                    record.getValuesOfColumns().add(new Int_DT(returnValue + columnCount));
                }
                record.assignSizeTo();
                manager.writeNewRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, record);
                return returnValue;
            } else {
                Errors.printMessage(String.format("Table '%s.%s' already exists.", databaseName, tableName));
                return -1;
            }
        }
        catch (InternalException e) {
            Errors.printMessage(e.getMessage());
            return -1;
        }
    }
//to update the internal columns of system table
    public boolean updateInternalColumns(String databaseName, String tableName, int startingRowId, List<InternalColumn> columns) {
        try {
        /*
         * System Tables Table Schema:
         * Column_no    Name                                    Data_type
         *      1       rowid                                   INT
         *      2       database_name                           TEXT
         *      3       table_name                              TEXT
         *      4       column_name                             TEXT
         *      5       data_type                               TEXT
         *      6       column_key                              TEXT
         *      7       ordinal_position                        TINYINT
         *      8       is_nullable                             TEXT
         */
            IO_Handling manager = new IO_Handling();
            if (columns != null && columns.size() == 0) return false;
            int i = 0;
            for (; i < columns.size(); i++) {
                DataRecord record = new DataRecord();
                record.setRowId(startingRowId++);
                record.getValuesOfColumns().add(new Int_DT(record.getRowId()));
                record.getValuesOfColumns().add(new Text_DT(databaseName));
                record.getValuesOfColumns().add(new Text_DT(tableName));
                record.getValuesOfColumns().add(new Text_DT(columns.get(i).getName()));
                record.getValuesOfColumns().add(new Text_DT(columns.get(i).getDataType()));
                record.getValuesOfColumns().add(new Text_DT(columns.get(i).getStringIsPrimary()));
                record.getValuesOfColumns().add(new Int_DT(i + 1));
                record.getValuesOfColumns().add(new Text_DT(columns.get(i).getStringIsNullable()));
                record.assignSizeTo();
                if (!manager.writeNewRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, record)) {
                    break;
                }
            }
            return true;
        }
        catch (InternalException e) {
            Errors.printMessage(e.getMessage());
        }
        return false;
    }
//increment the rows in the table
    public static int incrementTableRowCount(String databaseName, String tableName) {
        return updateRowCountInTable(databaseName, tableName, 1);
    }
//decrement the rows in the table
    public static int decrementTableRowCount(String databaseName, String tableName) {
        return updateRowCountInTable(databaseName, tableName, -1);
    }
//update rows in the table
    private static int updateRowCountInTable(String databaseName, String tableName, int rowCount) {
        try {
            IO_Handling manager = new IO_Handling();
            List<InternalCondition> conditions = new ArrayList<>();
            conditions.add(InternalCondition.CreateCondition(DatabaseUtility.TABLES_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new Text_DT(databaseName)));
            conditions.add(InternalCondition.CreateCondition(DatabaseUtility.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));
            List<Byte> updateColumnsIndexList = new ArrayList<>();
            updateColumnsIndexList.add(DatabaseUtility.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            List<Object> updateValueList = new ArrayList<>();
            updateValueList.add(new Int_DT(rowCount));
            return manager.updateRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_TABLES_TABLENAME, conditions, updateColumnsIndexList, updateValueList, true);
        }
        catch (InternalException e) {
            Errors.printMessage(e.getMessage());
        }
        return -1;
    }
}

