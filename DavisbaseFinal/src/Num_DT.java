import java.text.SimpleDateFormat;
import java.util.Date;
//base class and inheritance for numeric datatypes
public abstract class Num_DT<T> extends DataType<T> {

    //Comparison keywords;
    public static final short EQUALS = 0;
    public static final short LESS_THAN = 1;
    public static final short GREATER_THAN = 2;
    public static final short LESS_THAN_EQUALS = 3;
    public static final short GREATER_THAN_EQUALS = 4;

    protected final byte SIZE;

    protected Num_DT(int valueSerialCode, int nullSerialCode, int size) {
        super(valueSerialCode, nullSerialCode);
        this.SIZE = (byte) size;
    }

    public byte getSerialCode() {
        if(isNull)
            return nullSerialCode;
        else
            return valueSerialCode;
    }

    public byte getSIZE() {
        return SIZE;
    }

    public abstract void increment(T value);

    public abstract boolean compare(Num_DT<T> object2, short condition);
}

class BigInt_DT extends Num_DT<Long> {

    public BigInt_DT() {
        this(0, true);
    }

    public BigInt_DT(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public BigInt_DT(long value, boolean isNull) {
        super(DatabaseDefinedConstants.BIG_INT_SERIAL_TYPE_CODE, DatabaseDefinedConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Long.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Long value) {
        this.value += value;
    }

    @Override
    public boolean compare(Num_DT<Long> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return value == object2.getValue();

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case Num_DT.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }

    public boolean compare(TinyInt_DT object2, short condition) {
        BigInt_DT object = new BigInt_DT(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(SmallInt_DT object2, short condition) {
        BigInt_DT object = new BigInt_DT(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(Int_DT object2, short condition) {
        BigInt_DT object = new BigInt_DT(object2.getValue(), false);
        return this.compare(object, condition);
    }
}

class Int_DT extends Num_DT<Integer> {

    public Int_DT() {
        this(0, true);
    }

    public Int_DT(Integer value) {
        this(value == null ? 0 : value, value == null);
    }

    public Int_DT(int value, boolean isNull) {
        super(DatabaseDefinedConstants.INT_SERIAL_TYPE_CODE, DatabaseDefinedConstants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE, Integer.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Integer value) {
        this.value += value;
    }

    @Override
    public boolean compare(Num_DT<Integer> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return value == object2.getValue();

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case Num_DT.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }

    public boolean compare(TinyInt_DT object2, short condition) {
        Int_DT object = new Int_DT(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(SmallInt_DT object2, short condition) {
        Int_DT object = new Int_DT(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(BigInt_DT object2, short condition) {
    	BigInt_DT object = new BigInt_DT(value, false);
        return object.compare(object2, condition);
    }
}

class Date_DT extends Num_DT<Long> {

    public Date_DT() {
        this(0, true);
    }

    public Date_DT(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public Date_DT(long value, boolean isNull) {
        super(DatabaseDefinedConstants.DATE_SERIAL_TYPE_CODE, DatabaseDefinedConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Long.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    public String getStringValue() {
        Date date = new Date(this.value);
        return new SimpleDateFormat("MM-dd-yyyy").format(date);
    }

    @Override
    public void increment(Long value) {
        this.value += value;
    }

    @Override
    public boolean compare(Num_DT<Long> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return value == object2.getValue();

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case Num_DT.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }
}

class DateTime_DT extends Num_DT<Long> {

    public DateTime_DT() {
        this(0, true);
    }

    public DateTime_DT(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public DateTime_DT(long value, boolean isNull) {
        super(DatabaseDefinedConstants.DATE_TIME_SERIAL_TYPE_CODE, DatabaseDefinedConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Long.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    public String getStringValue() {
        Date date = new Date(this.value);
        return date.toString();
    }

    @Override
    public void increment(Long value) {
        this.value += value;
    }

    @Override
    public boolean compare(Num_DT<Long> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return value == object2.getValue();

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case Num_DT.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }
}

class Double_DT extends Num_DT<Double> {

    public Double_DT() {
        this(0, true);
    }

    public Double_DT(Double value) {
        this(value == null ? 0 : value, value == null);
    }

    public Double_DT(double value, boolean isNull) {
        super(DatabaseDefinedConstants.DOUBLE_SERIAL_TYPE_CODE, DatabaseDefinedConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Double.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Double value) {
        this.value += value;
    }

    @Override
    public boolean compare(Num_DT<Double> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return Double.doubleToLongBits(value) == Double.doubleToLongBits(object2.getValue());

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return Double.doubleToLongBits(value) >= Double.doubleToLongBits(object2.getValue());

            case Num_DT.LESS_THAN_EQUALS:
                return Double.doubleToLongBits(value) <= Double.doubleToLongBits(object2.getValue());

            default:
                return false;
        }
    }

    public boolean compare(Real_DT object2, short condition) {
        Double_DT object = new Double_DT(object2.getValue(), false);
        return this.compare(object, condition);
    }

}

class Real_DT extends Num_DT<Float> {

    public Real_DT() {
        this(0, true);
    }

    public Real_DT(Float value) {
        this(value == null ? 0 : value, value == null);
    }

    public Real_DT(float value, boolean isNull) {
        super(DatabaseDefinedConstants.REAL_SERIAL_TYPE_CODE, DatabaseDefinedConstants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE, Float.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Float value) {
        this.value += value;
    }

    @Override
    public boolean compare(Num_DT<Float> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return Float.floatToIntBits(value) == Float.floatToIntBits(object2.getValue());

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return Float.floatToIntBits(value) >= Float.floatToIntBits(object2.getValue());

            case Num_DT.LESS_THAN_EQUALS:
                return Float.floatToIntBits(value) <= Float.floatToIntBits(object2.getValue());

            default:
                return false;
        }
    }

    public boolean compare(Double_DT object2, short condition) {
        Double_DT object = new Double_DT(value, false);
        return object.compare(object2, condition);
    }
}

class SmallInt_DT extends Num_DT<Short> {

    public SmallInt_DT() {
        this((short) 0, true);
    }

    public SmallInt_DT(Short value) {
        this(value == null ? 0 : value, value == null);
    }

    public SmallInt_DT(short value, boolean isNull) {
        super(DatabaseDefinedConstants.SMALL_INT_SERIAL_TYPE_CODE, DatabaseDefinedConstants.TWO_BYTE_NULL_SERIAL_TYPE_CODE, Short.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Short value) {
        this.value = (short)(this.value + value);
    }

    @Override
    public boolean compare(Num_DT<Short> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return value == object2.getValue();

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case Num_DT.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }

    public boolean compare(TinyInt_DT object2, short condition) {
        SmallInt_DT object = new SmallInt_DT(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(Int_DT object2, short condition) {
        Int_DT object = new Int_DT(value, false);
        return object.compare(object2, condition);
    }

    public boolean compare(BigInt_DT object2, short condition) {
        BigInt_DT object = new BigInt_DT(value, false);
        return object.compare(object2, condition);
    }
}

class TinyInt_DT extends Num_DT<Byte> {

    public TinyInt_DT() {
        this((byte) 0, true);
    }

    public TinyInt_DT(Byte value) {
        this(value == null ? 0 : value, value == null);
    }

    public TinyInt_DT(byte value, boolean isNull) {
        super(DatabaseDefinedConstants.TINY_INT_SERIAL_TYPE_CODE, DatabaseDefinedConstants.ONE_BYTE_NULL_SERIAL_TYPE_CODE, Byte.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Byte value) {
        this.value = (byte)(this.value + value);
    }

    @Override
    public boolean compare(Num_DT<Byte> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case Num_DT.EQUALS:
                return value == object2.getValue();

            case Num_DT.GREATER_THAN:
                return value > object2.getValue();

            case Num_DT.LESS_THAN:
                return value < object2.getValue();

            case Num_DT.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case Num_DT.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }

    public boolean compare(SmallInt_DT object2, short condition) {
        SmallInt_DT object = new SmallInt_DT(value, false);
        return object.compare(object2, condition);
    }

    public boolean compare(Int_DT object2, short condition) {
        Int_DT object = new Int_DT(value, false);
        return object.compare(object2, condition);
    }

    public boolean compare(BigInt_DT object2, short condition) {
        BigInt_DT object = new BigInt_DT(value, false);
        return object.compare(object2, condition);
    }
}

