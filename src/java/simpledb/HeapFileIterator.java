package simpledb;

import javax.xml.crypto.Data;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    public HeapFileIterator(TransactionId tid, HeapFile file) {
        this.tid = tid;
        this.tableId = file.getId();
        this.file = file;
        this.opened = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        bufferPool = Database.getBufferPool();
        rewind();
        opened = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next != null)
            return true;

        next = fetchNext();

        return next != null;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            Tuple ret = next;

            next = null;

            return ret;
        }

        throw new NoSuchElementException();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        curPage = 0;
        itr = getNextPageIterator();
    }

    @Override
    public void close() {
        opened = false;
        itr = null;
        bufferPool = null;
    }

    private BufferPool bufferPool;
    private Iterator<Tuple> itr;
    private Tuple next;
    private boolean opened;

    private final TransactionId tid;
    private final int tableId;
    private int curPage;
    private final HeapFile file;

    private Iterator<Tuple> getNextPageIterator() throws TransactionAbortedException, DbException {
        System.out.println("tableId: [" + tableId + "] get pgNo: [" + curPage + "] total pgcnt: " + file.numPages());

        if (curPage < file.numPages()) {
            Page page = bufferPool.getPage(
                    tid,
                    new HeapPageId(tableId, curPage++),
                    Permissions.READ_ONLY);

            if (page instanceof HeapPage)
                return ((HeapPage) page).iterator();
        }

        return null;
    }

    private Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (itr == null)
            return null;

        while (!itr.hasNext()) {
            itr = getNextPageIterator();
            if (itr == null)
                return null;
        }

        return itr.next();
    }
}
