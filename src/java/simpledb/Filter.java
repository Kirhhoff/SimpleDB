package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here

        this.predicate = p;
        this.child = child;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here

        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here

        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
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

        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws
            TransactionAbortedException, DbException {
        // some code goes here

        Tuple next;
        while (child.hasNext()) {
            next = child.next();
            if (predicate.filter(next))
                return next;
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

        if (children.length >= 1)
            resetChild(children[0]);
    }

    private final Predicate predicate;
    private OpIterator child;
    private TupleDesc td;

    private void resetChild(OpIterator child) {

        if (this.child == child)
            return;

        this.child = child;
        this.td = child.getTupleDesc();
    }
}
