package simpledb;

import java.io.IOException;

public abstract class UniqueOperator extends Operator {

    public UniqueOperator(TransactionId t, OpIterator child) {
        // some code goes here

        this.tid = t;
        this.child = child;
        this.result = null;
        this.resultTd = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here

        child.open();
        super.open();
    }

    public void close() {
        // some code goes here

        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here

        this.result = null;
        child.rewind();
    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        // do nothing if result has already been generated
        if (result == null) {

            result = resultTuple(performOperation());

            return result;
        }

        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here

        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here

        if (children.length >= 1 && children[0] != null)
            resetChild(children[0]);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here

        return resultTd;
    }

    protected abstract int performOperation() throws TransactionAbortedException, DbException;

    protected final TransactionId tid;
    protected OpIterator child;
    private final TupleDesc resultTd;
    private Tuple result;

    private void resetChild(OpIterator child) {
        this.child = child;
    }

    private Tuple resultTuple(int successfulOperation) {
        Tuple result = new Tuple(resultTd);

        result.setField(0, new IntField(successfulOperation));

        return result;
    }
}
