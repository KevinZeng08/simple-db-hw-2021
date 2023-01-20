package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;

    private Object groupAndAggregateVals;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        // NO-GROUPING
        if (gbfield == NO_GROUPING) {
            groupAndAggregateVals = new ArrayList<Integer>();
        }
        // gbfield INT-TYPE
        else if (gbfieldtype == Type.INT_TYPE) {
            groupAndAggregateVals = new HashMap<Integer, List<Integer>>();
        }
        // gbfield STRING-TYPE
        else if (gbfieldtype == Type.STRING_TYPE) {
            groupAndAggregateVals = new HashMap<String, List<Integer>>();
        }
    }

    public void setTupleDesc(TupleDesc td){
        this.td = td;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField aggField = (IntField) tup.getField(afield);
        if(aggField == null) return;
        int aggVal = aggField.getValue();
        // NO-GROUPING
        if (gbfield == NO_GROUPING) {
            List<Integer> list = (List<Integer>) groupAndAggregateVals;
            compute(list, aggVal);
        }
        // gbfield INT-TYPE
        else if (gbfieldtype == Type.INT_TYPE) {
            HashMap<Integer, List<Integer>> map = (HashMap<Integer, List<Integer>>) groupAndAggregateVals;
            int groupVal = ((IntField) tup.getField(gbfield)).getValue();
            List<Integer> temp; // aggregateVal to update
            if (map.containsKey(groupVal)) {
                temp = map.get(groupVal);
            } else {
                temp = new ArrayList<>();
                map.put(groupVal, temp);
            }
            compute(temp, aggVal);
        }
        // gbfield STRING-TYPE
        else if (gbfieldtype == Type.STRING_TYPE) {
            HashMap<String, List<Integer>> map = (HashMap<String, List<Integer>>) groupAndAggregateVals;
            String groupVal = ((StringField) tup.getField(gbfield)).getValue();
            List<Integer> temp;
            if (map.containsKey(groupVal)) {
                temp = map.get(groupVal);
            } else {
                temp = new ArrayList<>();
                map.put(groupVal, temp);
            }
            compute(temp, aggVal);
        }
    }

    private void compute(List<Integer> vals, int val) {
        switch (what) {
            case MIN:
                if (vals.isEmpty()) {
                    vals.add(val);
                } else {
                    vals.set(0, Math.min(vals.get(0), val));
                }
                break;
            case MAX:
                if (vals.isEmpty()) {
                    vals.add(val);
                } else {
                    vals.set(0, Math.max(vals.get(0), val));
                }
                break;
            case SUM:
                if (vals.isEmpty()) {
                    vals.add(val);
                } else {
                    vals.set(0, vals.get(0) + val);
                }
                break;
            case AVG:
                if (vals.isEmpty()) {
                    vals.add(val);
                    vals.add(1);
                } else {
                    vals.set(0, vals.get(0) + val);
                    vals.set(1, vals.get(1) + 1);
                }
                break;
            case COUNT:
                if (vals.isEmpty()) {
                    vals.add(1);
                } else {
                    vals.set(0, vals.get(0) + 1);
                }
                break;
            case SC_AVG:
            case SUM_COUNT:
        }
    }


    public class IntegerAggregatorIterator implements OpIterator {

        List<Tuple> tuples;
        Iterator<Tuple> it;

        // fill tuples in the constructor
        public IntegerAggregatorIterator() {
            tuples = new ArrayList<>();
            // NO-GROUPING
            if (gbfield == NO_GROUPING) {
                List<Integer> aggVals = (List<Integer>) groupAndAggregateVals;
                int aggVal = aggVals.get(0);
                if(what == Op.AVG) {
                    aggVal = aggVals.get(0) / aggVals.get(1);
                }
                Tuple t = new Tuple(td);
                t.setField(0, new IntField(aggVal));
                tuples.add(t);
            }
            // gbfield INT-TYPE
            else if (gbfieldtype == Type.INT_TYPE) {
                HashMap<Integer, List<Integer>> map = (HashMap<Integer, List<Integer>>) groupAndAggregateVals;
                for(Map.Entry<Integer,List<Integer>> entry : map.entrySet()) {
                    int groupVal = entry.getKey();
                    List<Integer> aggVals = entry.getValue();
                    int aggVal = aggVals.get(0);
                    if(what == Op.AVG) {
                        aggVal = aggVals.get(0) / aggVals.get(1);
                    }
                    Tuple t = new Tuple(td);
                    t.setField(0, new IntField(groupVal));
                    t.setField(1, new IntField(aggVal));
                    tuples.add(t);
                }
            }
            // gbfield STRING-TYPE
            else if(gbfieldtype == Type.STRING_TYPE) {
                HashMap<String,List<Integer>> map = (HashMap<String,List<Integer>>) groupAndAggregateVals;
                for(Map.Entry<String,List<Integer>> entry : map.entrySet()) {
                    String groupVal = entry.getKey();
                    List<Integer> aggVals = entry.getValue();
                    int aggVal = aggVals.get(0);
                    if(what == Op.AVG) {
                        aggVal = aggVals.get(0) / aggVals.get(1);
                    }
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
        return new IntegerAggregatorIterator();
    }

}
