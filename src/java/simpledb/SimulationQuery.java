package simpledb;

import java.io.File;

public class SimulationQuery {

    public static void main(String[] args) {

        // Table schema
        Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] fieldNames = {"What", "the", "hell"};
        TupleDesc td = new TupleDesc(types, fieldNames);

        // Table file
        HeapFile tableFile = new HeapFile(new File("fun_data_file.dat"), td);
        Database.getCatalog().addTable(tableFile, "FunTable");

        // Construct a query
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, tableFile.getId());

        try {
            scan.open();

            while (scan.hasNext())
                System.out.println(scan.next());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scan.close();
        }
    }
}
