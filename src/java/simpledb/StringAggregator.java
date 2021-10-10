package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator extends AbstractAggregator{

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        super(gbfield, gbfieldtype, afield, what);
    }

    @Override
    protected AggregateData dataInstance() {
        return new StringAggregateData();
    }
}

class StringAggregateData implements AggregateData {

    private int cnt = 0;

    @Override
    public void accumulate(Aggregator.Op op, int value) {
        assert(op.equals(Aggregator.Op.COUNT));
        cnt++;
    }

    @Override
    public int getResult(Aggregator.Op op) {
        assert(op.equals(Aggregator.Op.COUNT));
        return cnt;
    }
}
