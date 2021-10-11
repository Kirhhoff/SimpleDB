package simpledb;

import java.io.File;
import java.io.FileNotFoundException;

public class SimulationJoin {

    public static void main(String[] args) throws FileNotFoundException {
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] fieldNames = new String[]{"field1", "field2", "field3"};
        TupleDesc td = new TupleDesc(types, fieldNames);

        HeapFile table1 = new HeapFile(new File("join_data_file1.dat"), td);
        HeapFile table2 = new HeapFile(new File("join_data_file2.dat"), td);

        Catalog catalog = Database.getCatalog();
        catalog.addTable(table1, "table1");
        catalog.addTable(table2, "table2");

        TransactionId tid = new TransactionId();
        SeqScan seqScan1 = new SeqScan(tid, table1.getId());
        SeqScan seqScan2 = new SeqScan(tid, table2.getId());

        Filter sf1 = new Filter(
                new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1)), seqScan1);

        JoinPredicate jp = new JoinPredicate(1, Predicate.Op.EQUALS, 1);

        Join join = new Join(jp, sf1, seqScan2);

        try {
            join.open();
            while (join.hasNext()) {
                Tuple tuple = join.next();
                System.out.println(tuple);
            }
            join.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
