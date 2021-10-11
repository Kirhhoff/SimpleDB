package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) throws FileNotFoundException {
        // some code goes here

        this.file = f;
        this.td = td;
        this.tableId = f.getAbsolutePath().hashCode();
        this.appendPageCache = new HashMap<>();
        this.fio = new RandomAccessFile(file, "rw");
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here

        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here

        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here

        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here

        // get newly appended page from cache rather than I/O
        // remove after getting it, cuz buffer pool is now
        // responsible to flush it back
        if (!appendPageCache.isEmpty() && appendPageCache.containsKey(pid))
            return appendPageCache.remove(pid);

        try {
            byte[] fileContent = readRawPage(pid);
            return new HeapPage(toHeapPageId(pid), fileContent);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        PageId pid = page.getId();
        int pageNo = pid.getPageNumber();
        if (pid.getTableId() != tableId)
            throw new IOException();

        long offset = pageNo * (long)BufferPool.getPageSize();
        if (fio.getFilePointer() != offset)
            fio.seek(offset);

        byte[] rawData = page.getPageData();
        fio.write(rawData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        int pageSize = BufferPool.getPageSize();
        return  (int)((file.length() + pageSize - 1) / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        BufferPool bufferPool = Database.getBufferPool();
        HeapPage page;
        for (int pageNo = 0; pageNo < numPages(); pageNo++) {
            page = (HeapPage)bufferPool.getPage(tid, new HeapPageId(tableId, pageNo), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0)
                return rawInsertTupleToPage(t, page, tid);
        }

        return rawInsertTupleToPage(t, appendPage(), tid);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException, IOException {
        // some code goes here
        // not necessary for lab1

        HeapPageId pid = (HeapPageId)t.getRecordId().getPageId();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);

        ArrayList<Page> ret = new ArrayList<>();
        ret.add(page);

        return ret;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(tid, this);
    }

    private final File file;
    private final TupleDesc td;
    private final int tableId;
    private RandomAccessFile fio;

    private final Map<PageId, Page> appendPageCache;

    private byte[] readRawPage(PageId pid) throws IOException {
        int pageSize = BufferPool.getPageSize();
        byte[] fileContent = new byte[pageSize];

        long seekPos = pid.getPageNumber() * (long)pageSize;
        if (fio.getFilePointer() != seekPos)
            fio.seek(seekPos);

        int readBytes = fio.read(fileContent, 0, pageSize);
        if (readBytes < 0)
            throw new IOException();

        return fileContent;
    }

    private HeapPageId toHeapPageId(PageId pid) {

        if (pid instanceof HeapPageId)
            return (HeapPageId)pid;

        return new HeapPageId(tableId, pid.getPageNumber());
    }

    private ArrayList<Page> rawInsertTupleToPage(Tuple t, HeapPage page, TransactionId tid) throws DbException {
        page.insertTuple(t);
        page.markDirty(true, tid);

        ArrayList<Page> ret = new ArrayList<>();
        ret.add(page);

        return ret;
    }

    private HeapPage appendPage() throws IOException {
        // memory cache to avoid the unnecessary I/O
        // for newly appended page
        HeapPage page = rawAppendPage();
        appendPageCache.put(page.getId(), page);
        return page;
    }

    private HeapPage rawAppendPage() throws IOException {
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();

        return new HeapPage(new HeapPageId(tableId, numPages() - 1), HeapPage.createEmptyPageData());
    }
}

