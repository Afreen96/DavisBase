import javafx.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
//a class to perform select * from or select * from where query
public class SelectQ implements QueryInterface {
    public String databaseName;
    public String tableName;
    public ArrayList<String> columns;
    private boolean isSelectAll;
    private ArrayList<CheckCondition> conditions = new ArrayList<>();

    public SelectQ(String databaseName, String tableName, ArrayList<String> columns, ArrayList<CheckCondition> conditions, boolean isSelectAll) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = columns;
        this.conditions = conditions;
        this.isSelectAll = isSelectAll;
    }

    @Override
    public Result executeAQuery() {
        try {
            ResultSet resultSet = ResultSet.CreateResultSet();

            ArrayList<Record> records = GetData();
            resultSet.setColumns(this.columns);
            for (Record record : records) {
                resultSet.addRecord(record);
            }

            return resultSet;
        }
        catch (Exception e) {
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

            Pair<HashMap<String, Integer>, HashMap<Integer, String>> maps = mapOrdinalIdToColumnName(this.tableName);
            HashMap<String, Integer> columnToIdMap = maps.getKey();
            HashMap<String, Integer> columnDataTypeMapping = DatabaseUtilityHelper.getDatabaseUtilityHelper().getTableColumnDataTypes(this.databaseName, tableName);

            if (conditions != null) {
                List<String> retrievedColumns = DatabaseUtilityHelper.getDatabaseUtilityHelper().getAllTableColumns(this.databaseName, tableName);

                for (CheckCondition condition : conditions) {
                    if (!Errors.checkConditionValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, condition)) {
                        return false;
                    }
                }
            }

            if (this.columns != null) {
                for (String column : this.columns) {
                    if (!columnToIdMap.containsKey(column)) {
                        Errors.printMessage(String.format("ERROR(112CM): Unknown column '%s' in table '%s'", column, this.tableName));
                        return false;
                    }
                }
            }

            if (conditions != null) {
                for (CheckCondition condition : conditions) {
                    if (!columnToIdMap.containsKey(condition.column)) {
                        Errors.printMessage((String.format("ERROR(112CM): Unknown column '%s' in table '%s'", condition.column, this.tableName)));
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            Errors.printMessage(e.getMessage());
            return false;
        }
        return true;
    }

    private ArrayList<Record> GetData() throws Exception {
        ArrayList<Record> records = new ArrayList<>();
        Pair<HashMap<String, Integer>, HashMap<Integer, String>> maps = mapOrdinalIdToColumnName(this.tableName);
        HashMap<String, Integer> columnToIdMap = maps.getKey();
        ArrayList<Byte> columnsList = new ArrayList<>();
        List<DataRecord> internalRecords;
        IO_Handling manager = new IO_Handling();

        List<InternalCondition> conditions = new ArrayList<>();
        InternalCondition internalCondition = null;

        if(this.conditions != null){
            for(CheckCondition condition : this.conditions) {
                internalCondition = new InternalCondition();
                if (columnToIdMap.containsKey(condition.column)) {
                    internalCondition.setIndex(columnToIdMap.get(condition.column).byteValue());
                }

                DataType dataType = DataType.createDataType(condition.value);
                internalCondition.setValue(dataType);

                Short operatorShort = Errors.ConvertFromOperator(condition.operator);
                internalCondition.setConditionType(operatorShort);
                conditions.add(internalCondition);
            }
        }

        if(this.columns == null) {
            internalRecords = manager.findRecord(this.databaseName,
                    this.tableName, conditions, false);

            HashMap<Integer, String> idToColumnMap = maps.getValue();
            this.columns = new ArrayList<>();
            for (int i=0; i<columnToIdMap.size();i++) {
                if(idToColumnMap.containsKey(i)){
                    columnsList.add((byte)i);
                    this.columns.add(idToColumnMap.get(i));
                }
            }
        }
        else {
            for (String column : this.columns) {
                if (columnToIdMap.containsKey(column)) {
                    columnsList.add(columnToIdMap.get(column).byteValue());
                }
            }

            internalRecords = manager.findRecord(this.databaseName,
                    this.tableName, conditions, columnsList, false);
        }

        Byte[] columnIds = new Byte[columnsList.size()];
        int k = 0;
        for(Byte column : columnsList){
            columnIds[k] = column;
            k++;
        }

        HashMap<Integer, String> idToColumnMap = maps.getValue();
        for(DataRecord internalRecord : internalRecords){
            Object[] dataTypes = new DataType[internalRecord.getValuesOfColumns().size()];
            k=0;
            for(Object columnValue : internalRecord.getValuesOfColumns()){
                dataTypes[k] = columnValue;
                k++;
            }
            Record record = Record.createNewRecord();
            for(int i=0;i<columnIds.length;i++) {
                Literal literal;
                if(idToColumnMap.containsKey((int)columnIds[i])) {
                    literal = Literal.CreateLiteral((DataType)dataTypes[i], Errors.resolveClass(dataTypes[i]));
                    record.assign(idToColumnMap.get((int)columnIds[i]), literal);
                }
            }
            records.add(record);
        }

        return records;
    }


    private Pair<HashMap<String, Integer>, HashMap<Integer, String>> mapOrdinalIdToColumnName(String tableName) throws Exception {
        HashMap<Integer, String> idToColumnMap = new HashMap<>();
        HashMap<String, Integer> columnToIdMap = new HashMap<>();
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(DatabaseUtility.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new Text_DT(tableName)));

        IO_Handling manager = new IO_Handling();
        List<DataRecord> records = manager.findRecord(DatabaseDefinedConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseDefinedConstants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (int i = 0; i < records.size(); i++) {
            DataRecord record = records.get(i);
            Object object = record.getValuesOfColumns().get(DatabaseUtility.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            idToColumnMap.put(i, ((DataType) object).getStringValue());
            columnToIdMap.put(((DataType) object).getStringValue(), i);
        }

        return new Pair<>(columnToIdMap, idToColumnMap);
    }
}
