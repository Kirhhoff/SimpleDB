package simpledb;

public interface AggregateData {

    void accumulate(Aggregator.Op op, int value);

    int getResult(Aggregator.Op op);
}
