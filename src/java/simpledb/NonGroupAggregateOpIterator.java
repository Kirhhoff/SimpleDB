package simpledb;

import java.util.Arrays;
import java.util.Iterator;

public abstract class NonGroupAggregateOpIterator extends AggregateOpIterator {

    private final TupleDesc td;
    protected Tuple[] tupleAsArray;
    private Iterator<Tuple> itr;

    public NonGroupAggregateOpIterator() {
        td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    @Override
    public void open() throws TransactionAbortedException, DbException {
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(getAggregateResult()));
        tupleAsArray = new Tuple[]{tuple};

        rewind();
        super.open();
    }

    @Override
    protected Tuple readNext() {
        return itr.next();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return super.hasNext() && itr.hasNext();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        itr = Arrays.stream(tupleAsArray).iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void close() {
        super.close();
        itr = null;
        tupleAsArray = null;
    }

    abstract protected int getAggregateResult();
}
