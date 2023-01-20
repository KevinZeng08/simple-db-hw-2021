package simpledb.storage;

import simpledb.common.Type;

import javax.print.DocFlavor;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc _schema;
//    private List<Field> _fields;
    private Field[] _fields;
    private RecordId rid;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        if(td.numFields() < 1) return;
        this._schema = td;
//        _fields = new ArrayList<>(td.numFields());
        _fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return _schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        _fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return _fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
//        throw new UnsupportedOperationException("Implement this");
        StringBuilder res = new StringBuilder();
        for(Field field : _fields) {
            Type type = field.getType();
            if("INT_TYPE".equals(type.name())){
                IntField intField = (IntField)field;
                res.append(intField.getValue()).append("\t");
            }else if("STRING_TYPE".equals(type.name())){
                StringField stringField = (StringField)field;
                res.append(stringField.getValue()).append("\t");
            }
        }
        return res.toString();
    }

    public class FieldIterator implements Iterator<Field> {

        int curIdx = 0;

        @Override
        public boolean hasNext() {
            if(_fields.length > curIdx) {
                return true;
            }
            return false;
        }

        @Override
        public Field next() {
            return _fields[curIdx++];
        }
    }
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
//        List<Field> fields = Arrays.asList(_fields);
//        return fields.iterator();
        return new FieldIterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this._schema = td;
    }
}
