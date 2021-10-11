package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();

        freeSlots = calculateFreeSlots();
        lastModifierTid = null;
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here

        int pageSize = BufferPool.getPageSize();
        int tupleSize = td.getSize();

        return (pageSize * 8) / (tupleSize * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here

        return (numSlots + 7) / 8;
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here

        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1

        if (t == null || t.getRecordId() == null)
            throw new DbException("null tuple or null tuple recordId");

        int tupleNo = t.getRecordId().getTupleNumber();
        int tupleSlotNo = tupleNo % numSlots;

        if (!isSlotUsed(tupleSlotNo) || !tuples[tupleSlotNo].getRecordId().equals(t.getRecordId())) {
            if (!isSlotUsed(tupleNo))
                throw new DbException("tuple slot is already empty");
            else
                throw new DbException("tuple not on page");
        }

        tuples[tupleSlotNo] = null;

        markSlotUsed(tupleSlotNo, false);
        freeSlots++;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1

        if (!t.getTupleDesc().equals(td))
            throw new DbException("tuple descriptor is mismatch");

        int tupleSlotNo = nextFreeSlotNo();
        if (tupleSlotNo < 0)
            throw new DbException("page is full");

        freeSlots--;
        markSlotUsed(tupleSlotNo, true);

        t.setRecordId(new RecordId(pid, numSlots * pid.getPageNumber() + tupleSlotNo));
        tuples[tupleSlotNo] = t;
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	    // not necessary for lab1

        lastModifierTid = dirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	    // Not necessary for lab1

        // when the page gets clean, tid will then be set to null
        return lastModifierTid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here

        return freeSlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here

        int headerPos = i / 8;
        int bitmask = 1 << (i % 8);

        return (header[headerPos] & bitmask) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1

        int headerPos = i / 8;
        int bitmask = 1 << (i % 8);

        if (value)
            header[headerPos] |= bitmask;
        else
            header[headerPos] &= ~bitmask;
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here

        Tuple[] nonNullTuples = collectNonNullTuples();

        return Arrays.stream(nonNullTuples).iterator();
    }

    private int freeSlots;
    private TransactionId lastModifierTid;

    private int calculateFreeSlots() {

        byte b;
        int use = 0, cnt;
        for (int bIdx= 0; bIdx < numSlots / 8; bIdx++) {
            b = header[bIdx];
            cnt = 8;
            while (cnt-- > 0) {
                if ((b & 1) != 0)
                    use++;
                b >>= 1;
            }
        }

        // deal with last byte
        b = header[header.length - 1];
        cnt = numSlots % 8;
        while (cnt-- > 0) {
            if ((b & 1) != 0)
                use++;
            b >>= 1;
        }

        return numSlots - use;
    }

    private Tuple[] collectNonNullTuples() {
        int total = numSlots - getNumEmptySlots();
        Tuple[] ret = new Tuple[total];

        int headerPos = 0, retCnt = 0, tuplePos = 0;
        byte b, bitCnt;
        while (retCnt < total) {
            b = header[headerPos++];
            bitCnt = 8;
            while (bitCnt-- > 0 && retCnt < total) {
                if ((b & 1) != 0) {
                    ret[retCnt++] = tuples[tuplePos++];
                } else {
                    tuplePos++;
                }
            }
        }

        return ret;
    }

    private int nextFreeSlotNo() {
        if (freeSlots == 0)
            return -1;

        int tupleSlotNo = 0, headerPos = -1;
        while (header[++headerPos] == -1)
            tupleSlotNo += 8;

        byte b = header[headerPos];
        while ((b & 1) != 0) {
            b >>= 1;
            tupleSlotNo++;
        }

        return tupleSlotNo;
    }

    private Iterator<Tuple> lazyIterator() {
        return new Iterator<Tuple>() {
            private int accessCnt = 0;
            private final int total = numSlots - getNumEmptySlots();

            @Override
            public boolean hasNext() {
                return accessCnt < total;
            }

            private int bitCnt = 8;
            private int bitIdx = 0;
            private byte b = header[0];
            private int pos = 0;
            private int call_cnt = 0;
            @Override
            public Tuple next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                int bit;
                while (true) {
                    if (bitCnt == 0) {
                        b = header[++bitIdx];
                        while (b == 0) {
                            b = header[++bitIdx];
                            pos += 8;
                        }
                        bitCnt = 8;
                    }
                    while (bitCnt-- > 0) {
                        bit = b & 1;
                        b >>= 1;
                        if (bit != 0) {
                            accessCnt++;
                            return tuples[pos++];
                        }
                        pos++;
                    }
                }
            }
        };
    }
}

