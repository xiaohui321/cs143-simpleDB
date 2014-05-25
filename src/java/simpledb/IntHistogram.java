package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

    private final int max, min, num_buckets, buckets[];
    private int total_num_values;
    private final double bucket_width;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it
     * receives. It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time
     * through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are
     * both constant with respect to the number of values being histogrammed.
     * For example, you shouldn't simply store every value that you see in a
     * sorted list.
     * 
     * @param buckets
     *            The number of buckets to split the input value into.
     * @param min
     *            The minimum integer value that will ever be passed to this
     *            class for histogramming
     * @param max
     *            The maximum integer value that will ever be passed to this
     *            class for histogramming
     */
    public IntHistogram(final int buckets, final int min, final int max) {
	this.max = max;
	this.min = min;
	num_buckets = buckets;
	bucket_width = (double) (max - min) / num_buckets;
	this.buckets = new int[num_buckets];
	total_num_values = 0;
    }

    /**
     * calculate this value belongs to which bucket. If v==max, then set the
     * index = num_buckets - 1
     */
    private int calculateIndex(final int v) {
	if (v >= max)
	    return num_buckets - 1;
	else if (v <= min)
	    return 0;
	else
	    return (int) ((v - min) / bucket_width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * 
     * @param v
     *            Value to add to the histogram
     */
    public void addValue(final int v) {
	buckets[calculateIndex(v)]++;
	total_num_values++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this
     * table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate
     * of the fraction of elements that are greater than 5.
     * 
     * @param op
     *            Operator
     * @param v
     *            Value
     * @return Predicted selectivity of this particular operator and value
     * @throws ParsingException
     */
    public double estimateSelectivity(final Predicate.Op op, final int v) {
	int index = calculateIndex(v);

	double b_right, b_left, count;
	switch (op) {
	    case EQUALS:
		return buckets[index] / bucket_width / total_num_values;

	    case NOT_EQUALS:
		return 1 - buckets[index] / bucket_width / total_num_values;

	    case GREATER_THAN:
		b_right = min + (index + 1) * bucket_width;
		count = buckets[index] * (b_right - v) / bucket_width;
		for (int i = index + 1; i < num_buckets; i++)
		    count += buckets[i];
		return count / total_num_values;

	    case GREATER_THAN_OR_EQ:
		b_right = min + (index + 1) * bucket_width;
		count = buckets[index] * (b_right - v + 1) / bucket_width;
		for (int i = index + 1; i < num_buckets; i++)
		    count += buckets[i];
		return count / total_num_values;

	    case LESS_THAN:
		b_left = min + index * bucket_width;
		count = buckets[index] * (v - b_left) / bucket_width;
		for (int i = 0; i < index; i++)
		    count += buckets[i];
		return count / total_num_values;

	    case LESS_THAN_OR_EQ:
		b_left = min + index * bucket_width;
		count = buckets[index] * (v - b_left + 1) / bucket_width;
		for (int i = 0; i < index; i++)
		    count += buckets[i];
		return count / total_num_values;
	    default:
		return -1;
	}
    }

    /**
     * @return the average selectivity of this histogram.
     * 
     *         This is not an indispensable method to implement the basic join
     *         optimization. It may be needed if you want to implement a more
     *         efficient optimization
     * */
    public double avgSelectivity() {
	// some code goes here
	return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
	String values = "";
	for (int i = 0; i < num_buckets; i++)
	    values += buckets[i];
	return "(There are " + num_buckets + " buckets from " + max + " to "
	        + min + "and the value for each buckets are: " + values + ")";
    }
}
