package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int range;
    private int ntups; // total number of tuples
    private Bucket[] dataStore;

    class Bucket {
//        int[] data;
        int left; // min value in this bucket
        int right;
        int width;
        int height; // total counter of values in this bucket
        // TODO add prefix[] and suffix[] to compute selectivity accurately

        public Bucket(int left, int width) {
            this.height = 0;
            this.left = left;
            this.width = width;
            this.right = left + width - 1;
//            this.data = new int[width];
        }

        void addValue(int v) {
//            data[v - left]++;
            height++;
        }

        public String toString() {
            StringBuilder res = new StringBuilder();
            String range = "[" + left + ", " + right + "]";
            res.append("range: ").append(range).append(" height: " + height);
            return res.toString();
        }
    }

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.ntups = 0;
        this.range = (max - min + 1) / buckets;
        if ((max - min + 1) % buckets != 0) this.range++;
        dataStore = new Bucket[buckets];
        for (int i = 0; i < buckets; i++) {
            dataStore[i] = new Bucket(i * range + min, range);
        }
    }

    private int getBucketIndex(int v) {
        if (v < min) return 0;
        if (v > max) return buckets - 1;
        return Math.abs(v - min) / range;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int bucketIdx = getBucketIndex(v);
        ntups++;
        dataStore[bucketIdx].addValue(v);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int bucketIdx = getBucketIndex(v);
        Bucket bucket = dataStore[bucketIdx];
        double selectivity = 1.0;
        double eq = 0.0,gt = 0.0 ,lt = 0.0;
        if(v >= min && v <= max) {
            eq = 1.0 * bucket.height / bucket.width / ntups;
            double partGt = (1.0 * bucket.height / ntups) * (bucket.right - v) / bucket.width;
            // buckets after this are all matched
            // TODO prefix sum?
            int sumGt = 0;
            for (int i = bucketIdx + 1; i < buckets; i++) {
                sumGt += dataStore[i].height;
            }
            double restGt = 1.0 * sumGt / ntups;
            gt = partGt + restGt;
            double partLt = (1.0 * bucket.height / ntups) * (v - bucket.left) / bucket.width;
            int sumLt = 0;
            for (int i = 0; i < bucketIdx; i++) {
                sumLt += dataStore[i].height;
            }
            double restLt = 1.0 * sumLt / ntups;
            lt = partLt + restLt;
        }
        switch (op) {
            case EQUALS:
                if (v >= min && v <= max)
//                    selectivity = 1.0 * bucket.height / bucket.width / ntups;
                {
                    selectivity = eq;
                } else selectivity = 0.0;
                break;
            case GREATER_THAN:
                if (v < min) {
                    selectivity = 1.0;
                } else if (v >= max) {
                    selectivity = 0.0;
                } else {
                    selectivity = gt;
                }
                break;
            case GREATER_THAN_OR_EQ:
                if (v <= min) {
                    selectivity = 1.0;
                } else if (v > max) {
                    selectivity = 0.0;
                } else {
                    selectivity = gt + eq;
                }
                break;
            case LESS_THAN:
                if(v <= min) {
                    selectivity = 0.0;
                } else if(v > max) {
                    selectivity = 1.0;
                } else {
                    selectivity = lt;
                }
                break;
            case LESS_THAN_OR_EQ:
                if(v < min) {
                    selectivity = 0.0;
                } else if(v >= max) {
                    selectivity = 1.0;
                } else {
                    selectivity = lt + eq;
                }
                break;
            case NOT_EQUALS:
                if (v >= min && v <= max) {
                    selectivity = 1.0 - eq;
                } else {
                    selectivity = 1.0;
                }
                break;
        }
        return selectivity;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < buckets; i++) {
            res.append("Bucket[" + i + "]").append("\n");
            res.append(dataStore[i].toString()).append("\n");
        }
        return res.toString();
    }
}
