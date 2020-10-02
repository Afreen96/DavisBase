import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
//class to insert into the database
public class InsertQ implements QueryInterface {
    public String tableName;
    public ArrayList<String> columns;
    private ArrayList<Literal> values;
    public String databaseName;

    public InsertQ(String databaseName, String tableName, ArrayList<String> columns, ArrayList<Literal> values) {
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
        this.databaseName = databaseName;
    }

    @Override
    public Result executeAQuery() {
        try {
            IO_Handling manager = new IO_Handling();
            List<String> retrievedColumns = DatabaseUtilityHelper.getDatabaseUtilityHelper().getAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = DatabaseUtilityHelper.getDatabaseUtilityHelper().getTableColumnDataTypes(this.databaseName, tableName);

            DataRecord record = new DataRecord();
            generateRecords(record.getValuesOfColumns(), columnDataTypeMapping, retrievedColumns);

            int rowID = findRowID(retrievedColumns);
            record.setRowId(rowID);
            record.assignSizeTo();

            Result result = null;
            boolean status = manager.writeNewRecord(this.databaseName, tableName, record);
            if (status) {
                result = new Result(1);
            } else {
                Errors.printMessage("ERROR(110F): Unable to insert record.");
            }

            return result;
        }
        catch (InternalException e) {
            Errors.printMessage(e.getMessage());
        }
        return null;
    }

    @Override
    public boolean validateAQuery() {
        try {
            IO_Handling manager = new IO_Handling();
            if (!manager.checkTblExists(this.databaseName, tableName)) {
                Errors.printMissingTableError(this.databaseName, tableName);
                return false;
            }

            List<String> retrievedColumns = DatabaseUtilityHelper.getDatabaseUtilityHelper().getAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = DatabaseUtilityHelper.getDatabaseUtilityHelper().getTableColumnDataTypes(this.databaseName, tableName);

            if (columns == null) {
                if (values.size() > retrievedColumns.size()) {
                    Errors.printMessage("ERROR(110C): ColumnTypeForTable count doesn't match value count at row 1");
                    return false;
                }

                Errors Errors = new Errors();
                if (!Errors.checkDataTypeValidity(columnDataTypeMapping, retrievedColumns, values)) {
                    return false;
                }
            } else {
                if (columns.size() > retrievedColumns.size()) {
                    Errors.printMessage("ERROR(110C): ColumnTypeForTable count doesn't match value count at row 1");
                    return false;
                }

                boolean areColumnsValid = checkColumnValidity(retrievedColumns);
                if (!areColumnsValid) {
                    return false;
                }

                boolean areColumnsDataTypeValid = validateColumnDataTypes(columnDataTypeMapping);
                if (!areColumnsDataTypeValid) {
                    return false;
                }
            }

            boolean isNullConstraintValid = checkNullConstraint(retrievedColumns);
            if (!isNullConstraintValid) {
                return false;
            }

            boolean isPrimaryKeyConstraintValid = checkPrimaryKeyConstraint(retrievedColumns);
            if (!isPrimaryKeyConstraintValid) {
                return false;
            }
        }
        catch (InternalException e) {
            Errors.printMessage(e.getMessage());
            return false;
        }

        return true;
    }

    private boolean validateColumnDataTypes(HashMap<String, Integer> columnDataTypeMapping) {
        return checkColumnDataTypeValidity(columnDataTypeMapping);
    }

    private boolean checkColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        for (String tableColumn : columns) {
            if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
                columnsValid = false;
                invalidColumn = tableColumn;
                break;
            }
        }

        if (!columnsValid) {
            Errors.printMessage("ERROR(110C): Invalid column '" + invalidColumn + "'");
            return false;
        }

        return true;
    }

    private boolean checkNullConstraint(List<String> retrievedColumnNames) throws InternalException {
        HashMap<String, Integer> columnsList = new HashMap<>();

        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                columnsList.put(columns.get(i), i);
            }
        }
        else {
            for (int i = 0; i < values.size(); i++) {
                columnsList.put(retrievedColumnNames.get(i), i);
            }
        }

        return DatabaseUtilityHelper.getDatabaseUtilityHelper().checkForNullConstraint(this.databaseName, tableName, columnsList);
    }

    private boolean checkPrimaryKeyConstraint(List<String> retrievedColumnNames) throws InternalException {

        String primaryKeyColumnName = DatabaseUtilityHelper.getDatabaseUtilityHelper().getPrimaryKeyOfTable(databaseName, tableName);
        List<String> columnList = (columns != null) ? columns : retrievedColumnNames;

        if (primaryKeyColumnName.length() > 0) {
                if (columnList.contains(primaryKeyColumnName.toLowerCase())) {
                    int primaryKeyIndex = columnList.indexOf(primaryKeyColumnName);
                    if (DatabaseUtilityHelper.getDatabaseUtilityHelper().checkIfPrimaryKeyExists(this.databaseName, tableName, Integer.parseInt(values.get(primaryKeyIndex).value))) {
                        Errors.printMessage("ERROR(110P): Duplicate entry '" + values.get(primaryKeyIndex).value + "' for key 'PRIMARY'");
                        return false;
                    }
                }
        }

        return true;
    }

    private boolean checkColumnDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping) {
        String invalidColumn = "";

        for (String columnName : columns) {
            int dataTypeIndex = columnDataTypeMapping.get(columnName);
            int idx = columns.indexOf(columnName);
            Literal literal = values.get(idx);

            if (literal.type != Errors.internalDataTypeToModelDataType((byte)dataTypeIndex)) {
                if (Errors.canUpdateLiteralDataType(literal, dataTypeIndex)) {
                    continue;
                }

                invalidColumn = columnName;
                break;
            }
        }

        boolean valid = invalidColumn.length() <= 0;

        if (!valid) {
            Errors.printMessage("ERROR(110CV): Invalid value for column '" + invalidColumn  + "'");
            return false;
        }

        return true;
    }

    private void generateRecords(List<Object> columnList, HashMap<String, Integer> columnDataTypeMapping, List<String> retrievedColumns) {
        for (int i=0; i < retrievedColumns.size(); i++) {
            String column = retrievedColumns.get(i);

            if (columns != null) {
                if (columns.contains(column)) {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();

                    int idx = columns.indexOf(column);

                    DataType obj = getDataTypeObject(dataType);
                    String val = values.get(idx).toString();

                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                } else {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();
                    DataType obj = getDataTypeObject(dataType);
                    obj.setNull(true);
                    columnList.add(obj);
                }
            }
            else {

                if (i < values.size()) {
                    Byte dataType = (byte) columnDataTypeMapping.get(column).intValue();

                    int columnIndex = retrievedColumns.indexOf(column);
                    DataType obj = getDataTypeObject(dataType);
                    String val = values.get(columnIndex).toString();

                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                }
                else {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();
                    DataType obj = getDataTypeObject(dataType);
                    obj.setNull(true);
                    columnList.add(obj);
                }
            }
        }
    }

    private DataType getDataTypeObject(byte dataType) {

        switch (dataType) {
            case DatabaseDefinedConstants.TINYINT: {
                return new TinyInt_DT();
            }
            case DatabaseDefinedConstants.SMALLINT: {
                return new SmallInt_DT();
            }
            case DatabaseDefinedConstants.INT: {
                return new Int_DT();
            }
            case DatabaseDefinedConstants.BIGINT: {
                return new BigInt_DT();
            }
            case DatabaseDefinedConstants.REAL: {
                return new Real_DT();
            }
            case DatabaseDefinedConstants.DOUBLE: {
                return new Double_DT();
            }
            case DatabaseDefinedConstants.DATE: {
                return new Date_DT();

            }
            case DatabaseDefinedConstants.DATETIME: {
                return new DateTime_DT();
            }
            case DatabaseDefinedConstants.TEXT: {
                return new Text_DT();
            }
            default: {
                return new Text_DT();
            }
        }
    }

    private Object getDataTypeValue(byte dataType, String value) {

        switch (dataType) {
            case DatabaseDefinedConstants.TINYINT: {
                return Byte.parseByte(value);
            }
            case DatabaseDefinedConstants.SMALLINT: {
                return Short.parseShort(value);
            }
            case DatabaseDefinedConstants.INT: {
                return Integer.parseInt(value);
            }
            case DatabaseDefinedConstants.BIGINT: {
                return Long.parseLong(value);
            }
            case DatabaseDefinedConstants.REAL: {
                return Float.parseFloat(value);
            }
            case DatabaseDefinedConstants.DOUBLE: {
                return Double.parseDouble(value);
            }
            case DatabaseDefinedConstants.DATE: {
                return Errors.getDateEpoc(value, true);
            }
            case DatabaseDefinedConstants.DATETIME: {
                return Errors.getDateEpoc(value, false);
            }
            case DatabaseDefinedConstants.TEXT: {
                return value;
            }
            default: {
                return value;
            }
        }
    }

    private int findRowID (List<String> retrievedList) throws InternalException {
        DatabaseUtilityHelper helper = DatabaseUtilityHelper.getDatabaseUtilityHelper();
        int rowCount = helper.getTableRecords(this.databaseName, tableName);
        String primaryKeyColumnName = helper.getPrimaryKeyOfTable(databaseName, tableName);
        if (primaryKeyColumnName.length() > 0) {
            int primaryKeyIndex = (columns != null) ? columns.indexOf(primaryKeyColumnName) : retrievedList.indexOf(primaryKeyColumnName);
            return Integer.parseInt(values.get(primaryKeyIndex).value);
        }
        else {
            return rowCount + 1;
        }
    }
}