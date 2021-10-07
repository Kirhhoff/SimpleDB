package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
@SuppressWarnings("ALL")
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable, Cloneable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        protected TDItem clone() {
            // directly reference fields rather than clone them
            // this may incur some subtle bugs...
            return new TDItem(fieldType, fieldName);
        }

        @Override
        public boolean equals(Object o) {

            if (this == o)
                return true;

            // type check
            if (!(o instanceof TDItem))
                return false;

            TDItem that = (TDItem)o;

            if (that.fieldType.equals(fieldType) && that.fieldName.equals(fieldName))
                return true;

            return false;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here

        return Arrays.stream(items).iterator();
    }

    private static final long serialVersionUID = 1L;
    private static final String ANON_FIELD_NAME = "(anonymous)";
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        // lengths of type array and field name array should match
        assert(typeAr.length != 0 && typeAr.length == fieldAr.length);

        items = new TDItem[typeAr.length];

        int i = 0;
        for (Type t: typeAr) {
            assert(t != null);

            items[i] = new TDItem(t, fieldAr[i] == null ? ANON_FIELD_NAME : fieldAr[i]);

            i++;
        }

        tupleSize = calculateTupleSize();
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here

        assert(typeAr != null);

        items = new TDItem[typeAr.length];

        int i = 0;
        for (Type t: typeAr) {
            assert(t != null);

            items[i] = new TDItem(t, ANON_FIELD_NAME);

            i++;
        }

        tupleSize = calculateTupleSize();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here

        return items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here

        checkIndexRange(i);

        return items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here

        checkIndexRange(i);

        return items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        // consider a HashMap for wide table
        int idx = 0;
        for (TDItem item: items) {

            if (item.fieldName.equals(name))
                return idx;

            idx++;
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here

        return tupleSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

        TDItem[] mergedItems = new TDItem[td1.numFields() + td2.numFields()];
        TDItem[] items1 = td1.items.clone();
        TDItem[] items2 = td2.items.clone();

        int cnt1, cnt2;

        for (cnt1 = 0; cnt1 < items1.length; cnt1++)
            mergedItems[cnt1] = items1[cnt1];

        for (cnt2 = 0; cnt2 < items2.length; cnt2++)
            mergedItems[cnt1 + cnt2] = items2[cnt2];

        return new TupleDesc(mergedItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here

        if (this == o)
            return true;

        // type check
        if (!(o instanceof TupleDesc))
            return false;

        TupleDesc that = (TupleDesc)o;

        // num&size check
        if (that.numFields() != numFields() || that.getSize() != getSize())
            return false;

        // item check
        int num = numFields();
        for (int i = 0; i < num; i++)
            if (!that.items[i].equals(items[i]))
                return false;

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here

        String ret = TDitem2IndexedString( 0);

        for (int i = 1; i < numFields(); i++)
            ret += ", " + TDitem2IndexedString(i);

        return ret;
    }

    private final TDItem[] items;
    private final int tupleSize;

    private TupleDesc(TDItem[] items) {
        this.items = items;
        tupleSize = calculateTupleSize();
    }

    private void checkIndexRange(int i) throws NoSuchElementException{
        if (i >= numFields())
            throw new NoSuchElementException();
    }

    private int calculateTupleSize() {
        int size = 0;
        for (TDItem item: items)
            size += item.fieldType.getLen();

        return size;
    }

    private String TDitem2IndexedString(int i) {
        TDItem item = items[i];

        return item.fieldType.toString() + "[" + i + "](" + item.fieldName + "[" + i + "])";
    }
}
