package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;

    private Object groupAndAggregateVals; // only support COUNT

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        if(gbfieldtype == null) {
            this.gbfield = NO_GROUPING;
        }
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        // NO-GROUPING
        if (gbfield == NO_GROUPING) {
            groupAndAggregateVals = new ArrayList<Integer>();
        }
        // gbfield INT-TYPE
        else if (gbfieldtype == Type.INT_TYPE) {
            groupAndAggregateVals = new HashMap<Integer, Integer>();
        }
        // gbfield STRING-TYPE
        else if (gbfieldtype == Type.STRING_TYPE) {
            groupAndAggregateVals = new HashMap<String, Integer>();
        }
    }

    public void setTupleDesc(TupleDesc td) {
        this.td = td;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // set TupleDesc

        StringField aggField = (StringField) tup.getField(afield);
        if(aggField == null) return;
        // NO-GROUPING
        if (gbfield == NO_GROUPING) {
            List<Integer> vals = (List<Integer>) groupAndAggregateVals;
            if (vals.isEmpty()) {
                vals.add(1);
            } else {
                vals.set(0, vals.get(0) + 1);
            }
        }
        // gbfield INT-TYPE
        else if (gbfieldtype == Type.INT_TYPE) {
            HashMap<Integer,Integer> map = (HashMap<Integer,Integer>) groupAndAggregateVals;
            int groupVal = ((IntField) tup.getField(gbfield)).getValue();
            map.put(groupVal,map.getOrDefault(groupVal,0) + 1);
        }
        // gbfield STRING-TYPE
        else if (gbfieldtype == Type.STRING_TYPE) {
            HashMap<String,Integer> map = (HashMap<String,Integer>) groupAndAggregateVals;
            String groupVal = ((StringField) tup.getField(gbfield)).getValue();
            map.put(groupVal,map.getOrDefault(groupVal,0) + 1);
        }
    }

    public class StringAggregatorIterator implements OpIterator {

        List<Tuple> tuples;
        Iterator<Tuple> it;

        // fill tuples in the constructor
        public StringAggregatorIterator() {
            tuples = new ArrayList<>();
            // NO-GROUPING
            if (gbfield == NO_GROUPING) {
                List<Integer> aggVals = (List<Integer>) groupAndAggregateVals;
                int aggVal = aggVals.get(0);
                Tuple t = new Tuple(td);
                t.setField(0, new IntField(aggVal));
                tuples.add(t);
            }
            // gbfield INT-TYPE
            else if (gbfieldtype == Type.INT_TYPE) {
                HashMap<Integer, Integer> map = (HashMap<Integer, Integer>) groupAndAggregateVals;
                for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
                    int groupVal = entry.getKey();
                    int aggVal = entry.getValue();
                    Tuple t = new Tuple(td);
                    t.setField(0, new IntField(groupVal));
                    t.setField(1, new IntField(aggVal));
                    tuples.add(t);
                }
            }
            // gbfield STRING-TYPE
            else if (gbfieldtype == Type.STRING_TYPE) {
                HashMap<String, Integer> map = (HashMap<String, Integer>) groupAndAggregateVals;
                for (Map.Entry<String, Integer> entry : map.entrySet()) {
                    String groupVal = entry.getKey();
                    int aggVal = entry.getValue();
                    Tuple t = new Tuple(td);
                    t.setField(0, new StringField(groupVal,groupVal.length()));
                    t.setField(1, new IntField(aggVal));
                    tuples.add(t);
                }
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = tuples.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            it = null;
        }
    }
    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        return new StringAggregatorIterator();
    }

}
