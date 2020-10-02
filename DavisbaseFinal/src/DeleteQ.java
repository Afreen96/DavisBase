import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//class to implement delete query
public class DeleteQ implements QueryInterface {
    public String databaseName;
    public String tableName;
    public ArrayList<CheckCondition> conditions;
    public boolean isInternal = false;

    public DeleteQ(String databaseName, String tableName, ArrayList<CheckCondition> conditions){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.conditions = conditions;
    }

    public DeleteQ(String databaseName, String tableName, ArrayList<CheckCondition> conditions, boolean isInternal){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.conditions = conditions;
        this.isInternal = isInternal;
    }

    @Override
    public Result executeAQuery() {

        try {
            int rowCount;
            IO_Handling manager = new IO_Handling();

            if (conditions == null) {
                rowCount = manager.deleteRecord(databaseName, tableName, (new ArrayList<>()));
            } else {
                List<InternalCondition> conditionList = new ArrayList<>();
                InternalCondition internalCondition;

                for (CheckCondition condition : this.conditions) {
                    internalCondition = new InternalCondition();
                    List<String> retrievedColumns = DatabaseUtilityHelper.getDatabaseUtilityHelper().getAllTableColumns(this.databaseName, tableName);
                    int idx = retrievedColumns.indexOf(condition.column);
                    internalCondition.setIndex((byte) idx);

                    DataType dataType = DataType.createDataType(condition.value);
                    internalCondition.setValue(dataType);

                    internalCondition.setConditionType(Errors.ConvertFromOperator(condition.operator));
                    conditionList.add(internalCondition);
                }

                rowCount = manager.deleteRecord(databaseName, tableName, conditionList);

            }

            return new Result(rowCount, this.isInternal);
        } catch (InternalException e) {
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

            if (this.conditions != null) {
                List<String> retrievedColumns = DatabaseUtilityHelper.getDatabaseUtilityHelper().getAllTableColumns(this.databaseName, tableName);
                HashMap<String, Integer> columnDataTypeMapping = DatabaseUtilityHelper.getDatabaseUtilityHelper().getTableColumnDataTypes(this.databaseName, tableName);

                for (CheckCondition condition : this.conditions) {
                    if (!checkForColumnValidity(retrievedColumns)) {
                        return false;
                    }

                    if (!Errors.checkConditionValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, condition)) {
                        return false;
                    }
                }
            }
        } catch (InternalException e) {
            Errors.printMessage(e.getMessage());
            return false;
        }
        return true;
    }


    private boolean checkForColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        for (CheckCondition condition : this.conditions) {
            String tableColumn = condition.column;
            if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
                columnsValid = false;
                invalidColumn = tableColumn;
            }

            if (!columnsValid) {
                Errors.printMessage("ERROR(106C): ColumnTypeForTable " + invalidColumn + " is not present in the table " + tableName + ".");
                return false;
            }
        }

        return true;
    }
}
