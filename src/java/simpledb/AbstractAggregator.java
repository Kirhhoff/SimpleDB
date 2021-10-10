package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

abstract public class AbstractAggregator implements Aggregator{

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public AbstractAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        this.groupByFieldIdx = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldIdx = afield;
        this.aggregateOp = what;
        this.isGrouping = (gbfieldtype != null) && (gbfield != NO_GROUPING);

        if (this.isGrouping) {
            nonGroupData = null;
            groupData = new HashMap<>();
        } else {
            nonGroupData = dataInstance();
            groupData = null;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        AggregateData aggregateData;

        if (isGrouping) {
            Field group = tup.getField(groupByFieldIdx);
            assert(group.getType().equals(groupByFieldType));

            aggregateData = getAggDataByGroup(group);
        } else
            aggregateData = nonGroupData;

        aggregate(aggregateData, tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        if (isGrouping)
            return new GroupAggregatorOpIterator(groupByFieldType) {
                @Override
                protected Iterator<Map.Entry<Field, AggregateData>> getEntryIterator() {
                    return groupData.entrySet().iterator();
                }

                @Override
                protected Op getOp() {
                    return aggregateOp;
                }
            };

        else
            return new NonGroupAggregateOpIterator() {
                @Override
                protected int getAggregateResult() {
                    return nonGroupData.getResult(aggregateOp);
                }
            };
    }

    abstract protected AggregateData dataInstance();

    protected final int groupByFieldIdx;
    protected final Type groupByFieldType;
    protected final int aggregateFieldIdx;
    protected final Op aggregateOp;
    protected final boolean isGrouping;

    protected final AggregateData nonGroupData;
    protected final Map<Field, AggregateData> groupData;

    private AggregateData getAggDataByGroup(Field group) {
        AggregateData aggregateData;

        if (!groupData.containsKey(group)) {
            aggregateData = dataInstance();
            groupData.put(group, aggregateData);
        } else
            aggregateData = groupData.get(group);

        return aggregateData;
    }

    private void aggregate(AggregateData data, Tuple tup) {
        Field f = tup.getField(aggregateFieldIdx);

        // try to pass an integer value to accumulate operation...
        // for StringField this is necessary(only count aggregation)
        // just to conform to interface signature
        int value = 0;
        if (f instanceof IntField)
            value = ((IntField)f).getValue();

        data.accumulate(aggregateOp, value);
    }
}
