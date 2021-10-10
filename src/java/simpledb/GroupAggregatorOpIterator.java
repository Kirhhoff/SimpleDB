package simpledb;

import java.util.Iterator;
import java.util.Map;

public abstract class GroupAggregatorOpIterator extends AggregateOpIterator{

    public GroupAggregatorOpIterator(Type groupByFieldType) {
        this.td = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE});
    }

    @Override
    public void open() throws TransactionAbortedException, DbException {
        rewind();
        super.open();
    }

    @Override
    protected Tuple readNext() {
        Map.Entry<Field, AggregateData> entry = itr.next();

        Tuple tuple = new Tuple(td);
        tuple.setField(0, entry.getKey());
        tuple.setField(1, new IntField(entry.getValue().getResult(getOp())));

        return tuple;
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        return super.hasNext() && itr.hasNext();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        itr = getEntryIterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void close() {
        super.close();
        itr = null;
    }

    private final TupleDesc td;
    private Iterator<Map.Entry<Field, AggregateData>> itr;

    protected abstract Iterator<Map.Entry<Field, AggregateData>> getEntryIterator();
    protected abstract Aggregator.Op getOp();
}
