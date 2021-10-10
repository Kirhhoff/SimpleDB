package simpledb;

import java.util.NoSuchElementException;

abstract public class AggregateOpIterator implements OpIterator {

    @Override
    public void open() throws DbException, TransactionAbortedException {
        opened = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!opened)
            throw new IllegalStateException();

        return true;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext())
            throw new NoSuchElementException();

        return readNext();
    }

    @Override
    public void close() {
        opened = false;
    }

    protected abstract Tuple readNext();

    private boolean opened = false;
}
