package simpledb;

/** Unique identifier for HeapPage objects. */
@SuppressWarnings("ALL")
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here

        this.tableId = tableId;
        this.pgNO = pgNo;
        this.hash = (String.valueOf(tableId) + pgNo).hashCode();
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here

        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here

        return pgNO;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here

        return hash;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here

        if (this == o)
            return true;

        if (!(o instanceof HeapPageId))
            return false;

        HeapPageId that = (HeapPageId)o;

        return tableId == that.tableId && pgNO == that.pgNO;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

    private final int tableId;
    private final int pgNO;
    private final int hash;
}
