package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends UniqueOperator{

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here

        super(t, child);
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    @Override
    protected int performOperation() throws TransactionAbortedException, DbException {
        int successfulDeletion = 0;

        BufferPool bufferPool = Database.getBufferPool();
        while (child.hasNext()) {
            try {
                bufferPool.deleteTuple(tid, child.next());
                successfulDeletion++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return successfulDeletion;
    }

}
