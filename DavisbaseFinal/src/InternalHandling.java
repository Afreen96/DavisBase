//class to manage internal tables of the system
class InternalColumn {

	    private int index;

	    private Object value;

	    private String name;

	    private String dataType;

	    private boolean isPrimary;

	    private boolean isNullable;

	    private byte ordinalPosition;

	    public InternalColumn() {

	    }

	    public InternalColumn(String name, String dataType, boolean isPrimary, boolean isNullable) {
	        this.name = name;
	        this.dataType = dataType;
	        this.isPrimary = isPrimary;
	        this.isNullable = isNullable;
	    }

	    public int getIndex() {
	        return index;
	    }

	    public void setIndex(int index) {
	        this.index = index;
	    }

	    public Object getValue() {
	        return value;
	    }

	    public void setValue(Object value) {
	        this.value = value;
	    }

	    public String getName() {
	        return name;
	    }

	    public void setName(String name) {
	        this.name = name;
	    }

	    public String getDataType() {
	        return dataType;
	    }

	    public void setDataType(String dataType) {
	        this.dataType = dataType;
	    }

	    public boolean isPrimary() {
	        return isPrimary;
	    }

	    public void setPrimary(boolean primary) {
	        isPrimary = primary;
	    }

	    public String getStringIsPrimary() {
	        return isPrimary ? "PRI" : null;
	    }

	    public boolean isNullable() {
	        return isNullable;
	    }

	    public void setNullable(boolean nullable) {
	        isNullable = nullable;
	    }

	    public String getStringIsNullable() {
	        return isNullable ? "YES" : "NO";
	    }

	    public byte getOrdinalPosition() {
	        return ordinalPosition;
	    }

	    public void setOrdinalPosition(byte ordinalPosition) {
	        this.ordinalPosition = ordinalPosition;
	    }
	}
	class InternalCondition {

	    public static final short EQUALS = 0;
	    public static final short LESS_THAN = 1;
	    public static final short GREATER_THAN = 2;
	    public static final short LESS_THAN_EQUALS = 3;
	    public static final short GREATER_THAN_EQUALS = 4;

	    private byte index;

	    private short conditionType;

	    private Object value;

	    public static InternalCondition CreateCondition(byte index, short conditionType, Object value) {
	        InternalCondition condition = new InternalCondition(index, conditionType, value);
	        return condition;
	    }

	    public static InternalCondition CreateCondition(int index, short conditionType, Object value) {
	        InternalCondition condition = new InternalCondition(index, conditionType, value);
	        return condition;
	    }

	    public InternalCondition() {}

	    private InternalCondition(byte index, short conditionType, Object value) {
	        this.index = index;
	        this.conditionType = conditionType;
	        this.value = value;
	    }

	    private InternalCondition(int index, short conditionType, Object value) {
	        this.index = (byte) index;
	        this.conditionType = conditionType;
	        this.value = value;
	    }

	    public byte getIndex() {
	        return index;
	    }

	    public void setIndex(byte index) {
	        this.index = index;
	    }

	    public short getConditionType() {
	        return conditionType;
	    }

	    public void setConditionType(short conditionType) {
	        this.conditionType = conditionType;
	    }

	    public Object getValue() {
	        return value;
	    }

	    public void setValue(Object value) {
	        this.value = value;
	    }
	}
	
class InternalException extends Exception {

	    public static String BASE_ERROR_STRING = "ERROR(200): ";
	    public static String INVALID_DATATYPE_EXCEPTION = BASE_ERROR_STRING + "Invalid datatype given.";
	    public static String DATATYPE_MISMATCH_EXCEPTION = BASE_ERROR_STRING + "Invalid datatype given in WHERE clause. Expected %1.";
	    public static String INVALID_CONDITION_EXCEPTION = BASE_ERROR_STRING + "Invalid condition given. Currently only %1 supported.";
	    public static String GENERIC_EXCEPTION = BASE_ERROR_STRING + "An error was encountered while performing the given operation.";

	    public InternalException(String message, String parameter) {
	        super(message.replace("%1", parameter));
	    }

	    public InternalException(String message) {
	        super(message);
	    }

	}

