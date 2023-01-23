package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.index.BTreeFile;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;
    private int ioCostPerPage;
    private int ntups;
    private int npages;
    // every field has a histogram
//    private List<Object> histograms;
    private Object[] histograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        // cast to HeapFile to call numPages()
        if(dbFile instanceof HeapFile) {
            HeapFile hf = (HeapFile)dbFile;
            this.npages = hf.numPages();
        }
        TupleDesc td = dbFile.getTupleDesc();
        this.histograms = new Object[td.numFields()];
        // create one seqscan
        SeqScan seqScan = new SeqScan(null,tableid);
        // find min and max value for every field (scan once)
        int[] min = new int[td.numFields()];
        Arrays.fill(min,Integer.MAX_VALUE);
        int[] max = new int[td.numFields()];
        Arrays.fill(max,Integer.MIN_VALUE);
        try {
            seqScan.open();
            while(seqScan.hasNext()) {
                Tuple tup = seqScan.next();
                for(int i = 0;i < td.numFields(); i++) {
                    if(td.getFieldType(i) == Type.INT_TYPE) {
                        IntField f = (IntField) tup.getField(i);
                        min[i] = Math.min(min[i], f.getValue());
                        max[i] = Math.max(max[i], f.getValue());
                    }
                }
                this.ntups++; // count Tuples
            }
            seqScan.rewind();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        // construct histograms
        for (int i = 0; i < td.numFields(); i++) {
            if(td.getFieldType(i) == Type.INT_TYPE) {
                histograms[i] = new IntHistogram(NUM_HIST_BINS,min[i],max[i]);
            } else if(td.getFieldType(i) == Type.STRING_TYPE) {
                histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }
        // populate histograms by reading every tuple (scan once)
        try {
            seqScan.open();
            while(seqScan.hasNext()) {
                Tuple tup = seqScan.next();
                for(int i = 0;i < td.numFields(); i++) {
                    if(td.getFieldType(i) == Type.INT_TYPE) {
                        IntHistogram histogram = (IntHistogram)histograms[i];
                        IntField f = (IntField) tup.getField(i);
                        histogram.addValue(f.getValue());
                    } else if(td.getFieldType(i) == Type.STRING_TYPE) {
                        StringHistogram histogram = (StringHistogram)histograms[i];
                        StringField f = (StringField) tup.getField(i);
                        histogram.addValue(f.getValue());
                    }
                }
            }
            seqScan.close();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return npages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        double selectivity = 0.0;
        if(td.getFieldType(field) == Type.INT_TYPE) {
            IntHistogram histogram = (IntHistogram) histograms[field];
            IntField f = (IntField) constant;
            selectivity = histogram.estimateSelectivity(op,f.getValue());
        } else if(td.getFieldType(field) == Type.STRING_TYPE) {
            StringHistogram histogram = (StringHistogram) histograms[field];
            StringField f = (StringField) constant;
            selectivity = histogram.estimateSelectivity(op,f.getValue());
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntups;
    }

}
