package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> _items;
    private int _numFields;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return _items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        _items = new ArrayList<>();
        _numFields = typeAr.length;
        for (int i = 0; i < _numFields; i++) {
            TDItem item;
            if(fieldAr == null){
                item = new TDItem(typeAr[i],null);
            }else {
                item = new TDItem(typeAr[i],fieldAr[i]);
            }
            _items.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr,null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return _numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= numFields()){
            throw new NoSuchElementException();
        }
        return _items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i >= numFields()){
            throw new NoSuchElementException();
        }
        return _items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i = 0;i<_items.size();i++){
            String fieldName = _items.get(i).fieldName;
            // name is null
            if(name == null && fieldName == null) {
                return i;
            }
            // name is not null
            if(name != null && name.equals(fieldName)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : _items) {
            Type fieldType = item.fieldType;
//            String fieldName = item.fieldName;
            size += fieldType.getLen();
//            size += fieldName.length();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int numFields = 0;
        Iterator<TDItem> it1 = null;
        Iterator<TDItem> it2 = null;
        if(td1 != null) {
            numFields += td1.numFields();
            it1=td1.iterator();
        }
        if(td2 != null) {
            numFields += td2.numFields();
            it2 = td2.iterator();
        }
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];
        int idx = 0;
        while(it1 != null && it1.hasNext()){
            TDItem tdItem1 = it1.next();
            typeAr[idx] = tdItem1.fieldType;
            fieldAr[idx] = tdItem1.fieldName;
            idx++;
        }
        while(it2 != null && it2.hasNext()) {
            TDItem tdItem2 = it2.next();
            typeAr[idx] = tdItem2.fieldType;
            fieldAr[idx] = tdItem2.fieldName;
            idx ++;
        }
        TupleDesc res = new TupleDesc(typeAr,fieldAr);
        return res;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(o == null || !(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc td = (TupleDesc)o;
        if(this._numFields != td.numFields()){
            return false;
        }
        Iterator<TDItem> currentIt = this.iterator();
        Iterator<TDItem> compareIt = td.iterator();
        while(compareIt.hasNext()){
            TDItem compare = compareIt.next();
            TDItem current = currentIt.next();
            if(!compare.fieldType.name().equals(current.fieldType.name())) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder res = new StringBuilder();
        for(TDItem item : _items) {
            Type fieldType = item.fieldType;
            String fieldName = item.fieldName;
            res.append(fieldType.name()).append("(").append(fieldName).append("),");
        }
        res.deleteCharAt(res.length()-1);
        return res.toString();
    }
}
