package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator extends AbstractAggregator {

    private static final long serialVersionUID = 1L;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        super(gbfield, gbfieldtype, afield, what);
    }

    @Override
    protected AggregateData dataInstance() {
        return IntAggregateData.instance(aggregateOp);
    }
}

class IntAggregateData implements AggregateData{

    int value;
    int cnt;

    private IntAggregateData(int initValue) {
        this.value = initValue;
    }

    private IntAggregateData() {
        this.value = 0;
        this.cnt = 0;
    }

    public static IntAggregateData instance(Aggregator.Op op) {
        switch (op) {
            case MAX: return new IntAggregateData(Integer.MIN_VALUE);
            case MIN: return new IntAggregateData(Integer.MAX_VALUE);
            default: return new IntAggregateData();
        }
    }

    @Override
    public void accumulate(Aggregator.Op op, int value) {
        switch (op) {
            case MAX:
                if (value > this.value)
                    this.value = value;
                break;
            case MIN:
                if (value < this.value)
                    this.value = value;
                break;
            case SUM :
                this.value += value;
                break;
            case COUNT:
                cnt++;
                break;
            case AVG:
                this.value += value;
                cnt++;
                break;
        }
    }

    @Override
    public int getResult(Aggregator.Op op) {
        switch (op) {
            case AVG: return this.value / this.cnt;
            case COUNT: return this.cnt;
            default: return this.value;
        }
    }
}