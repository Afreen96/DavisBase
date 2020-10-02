//a parser class and enum to parse the SQL commands
enum CodesForOperator {
    EQUALS,
    GREATER_THAN,
    LESS_THAN,
    GREATER_THAN_EQUAL,
    LESS_THAN_EQUAL
}
class CheckCondition {
    public String column;
    public CodesForOperator operator;
    public Literal value;

    public static CheckCondition conditionCreate(String conditionString) {
        CodesForOperator operator = GetOperator(conditionString);
        if(operator == null) {
            QueryHandling.commandUnrecognised(conditionString, "Unrecognised operator. \nValid operators include =, >, <, >=, <=. \nPlease follow <column> <operator> <value>");
            return null;
        }

        CheckCondition condition = null;

        switch (operator){
            case GREATER_THAN:
                condition = getInternalCondition(conditionString, operator, ">");
                break;
            case LESS_THAN:
                condition = getInternalCondition(conditionString, operator, "<");
                break;
            case LESS_THAN_EQUAL:
                condition = getInternalCondition(conditionString, operator, "<=");
                break;
            case GREATER_THAN_EQUAL:
                condition = getInternalCondition(conditionString, operator, ">=");
                break;
            case EQUALS:
                condition = getInternalCondition(conditionString, operator, "=");
                break;
        }

        return condition;
    }

    private static CheckCondition getInternalCondition(String conditionString, CodesForOperator operator, String operatorString) {
        String[] parts;
        String column;
        Literal literal;
        CheckCondition condition;
        parts = conditionString.split(operatorString);
        if(parts.length != 2) {
            QueryHandling.commandUnrecognised(conditionString, "Unrecognised condition. Please follow <column> <operator> <value>");
            return null;
        }

        column = parts[0].trim();
        literal = Literal.CreateLiteral(parts[1].trim());

        if (literal == null) {
            return null;
        }

        condition = new CheckCondition(column, operator, literal);
        return condition;
    }

    private CheckCondition(String column, CodesForOperator operator, Literal value){
        this.column = column;
        this.operator = operator;
        this.value = value;
    }

    private static CodesForOperator GetOperator(String conditionString) {

        if(conditionString.contains("<=")){
            return CodesForOperator.LESS_THAN_EQUAL;
        }

        if(conditionString.contains(">=")){
            return CodesForOperator.GREATER_THAN_EQUAL;
        }

        if(conditionString.contains(">")){
            return CodesForOperator.GREATER_THAN;
        }

        if(conditionString.contains("<")){
            return CodesForOperator.LESS_THAN;
        }

        if(conditionString.contains("=")){
            return CodesForOperator.EQUALS;
        }

        return null;
    }
}

class Literal {
    public Enum_DT type;
    public String value;

    public static Literal CreateLiteral(DataType value, Byte type) {
        if(type == DatabaseDefinedConstants.INVALID_CLASS) {
            return null;
        }
        else if (value.isNull()) {
            return new Literal(Enum_DT.DOUBLE_DATETIME_NULL, value.getStringValue());
        }

        switch(type) {
            case DatabaseDefinedConstants.TINYINT:
                return new Literal(Enum_DT.TINYINT, value.getStringValue());
            case DatabaseDefinedConstants.SMALLINT:
                return new Literal(Enum_DT.SMALLINT, value.getStringValue());
            case DatabaseDefinedConstants.INT:
                return new Literal(Enum_DT.INT, value.getStringValue());
            case DatabaseDefinedConstants.BIGINT:
                return new Literal(Enum_DT.BIGINT, value.getStringValue());
            case DatabaseDefinedConstants.REAL:
                return new Literal(Enum_DT.REAL, value.getStringValue());
            case DatabaseDefinedConstants.DOUBLE:
                return new Literal(Enum_DT.DOUBLE, value.getStringValue());
            case DatabaseDefinedConstants.DATE:
                return new Literal(Enum_DT.DATE, Errors.getDateEpocAsString((long)value.getValue(), true));
            case DatabaseDefinedConstants.DATETIME:
                return new Literal(Enum_DT.DATETIME, Errors.getDateEpocAsString((long)value.getValue(), false));
            case DatabaseDefinedConstants.TEXT:
                return new Literal(Enum_DT.TEXT, value.getStringValue());
        }

        return null;
    }

    public static Literal CreateLiteral(String literalString){
        if(literalString.startsWith("'") && literalString.endsWith("'")){
            literalString = literalString.substring(1, literalString.length()-1);

            if (Errors.isvalidDateTimeFormat(literalString)) {
                return new Literal(Enum_DT.DATETIME, literalString);
            }

            if (Errors.isvalidDateFormat(literalString)) {
                return new Literal(Enum_DT.DATE, literalString);
            }

            return new Literal(Enum_DT.TEXT, literalString);
        }

        if(literalString.startsWith("\"") && literalString.endsWith("\"")){
            literalString = literalString.substring(1, literalString.length()-1);

            if (Errors.isvalidDateTimeFormat(literalString)) {
                return new Literal(Enum_DT.DATETIME, literalString);
            }

            if (Errors.isvalidDateFormat(literalString)) {
                return new Literal(Enum_DT.DATE, literalString);
            }

            return new Literal(Enum_DT.TEXT, literalString);
        }

        try{
            Integer.parseInt(literalString);
            return new Literal(Enum_DT.INT, literalString);
        }
        catch (Exception e){}

        try{
            Long.parseLong(literalString);
            return new Literal(Enum_DT.BIGINT, literalString);
        }
        catch (Exception e){}

        try{
            Double.parseDouble(literalString);
            return new Literal(Enum_DT.REAL, literalString);
        }
        catch (Exception e){}

            QueryHandling.commandUnrecognised(literalString, "Unrecognised Literal Found. Please use integers, real or strings ");
        return null;
    }

    private Literal(Enum_DT type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        if (this.type == Enum_DT.TEXT) {
            return this.value;
        } else if (this.type == Enum_DT.INT || this.type == Enum_DT.TINYINT ||
                this.type == Enum_DT.SMALLINT || this.type == Enum_DT.BIGINT) {
            return this.value;
        } else if (this.type == Enum_DT.REAL || this.type == Enum_DT.DOUBLE) {
            return String.format("%.2f", Double.parseDouble(this.value));
        } else if (this.type == Enum_DT.INT_REAL_NULL || this.type == Enum_DT.SMALL_INT_NULL || this.type == Enum_DT.TINY_INT_NULL || this.type == Enum_DT.DOUBLE_DATETIME_NULL) {
            return "NULL";
        } else if (this.type == Enum_DT.DATE || this.type == Enum_DT.DATETIME) {
            return this.value;
        }

        return "";
    }
}



