package simpledb;

import javax.xml.crypto.Data;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        this.tid = tid;
        this.tableId = file.getId();
        this.numPages = file.numPages();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        bufferPool = Database.getBufferPool();
        rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return itr != null && (itr.hasNext() || curPage < numPages);
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext())
            throw new NoSuchElementException();

        if (!itr.hasNext())
            itr = getNextPageIterator();

        return itr.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        curPage = 0;
        itr = getNextPageIterator();
    }

    @Override
    public void close() {
        itr = null;
        bufferPool = null;
    }

    private BufferPool bufferPool;
    private Iterator<Tuple> itr;

    private TransactionId tid;
    private int tableId;
    private final int numPages;
    private int curPage;

    private Iterator<Tuple> getNextPageIterator() throws TransactionAbortedException, DbException {
        Page page = bufferPool.getPage(
                tid,
                new HeapPageId(tableId, curPage++),
                Permissions.READ_ONLY);

        if (page instanceof HeapPage)
            return ((HeapPage)page).iterator();

        throw new TransactionAbortedException();
    }
}
