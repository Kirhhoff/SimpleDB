#!/bin/bash

# lab1
ant runtest -Dtest=TupleTest
ant runtest -Dtest=TupleDescTest
ant runtest -Dtest=CatalogTest
ant runtest -Dtest=HeapPageIdTest
ant runtest -Dtest=HeapPageReadTest
ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapFileReadTest

# lab2
ant runtest -Dtest=PredicateTest
ant runtest -Dtest=JoinPredicateTest
ant runtest -Dtest=FilterTest
ant runtest -Dtest=JoinTest
ant runsystest -Dtest=FilterTest
ant runsystest -Dtest=JoinTest
ant runtest -Dtest=IntegerAggregatorTest
ant runtest -Dtest=StringAggregatorTest
ant runtest -Dtest=AggregateTest
ant runsystest -Dtest=AggregateTest
ant runtest -Dtest=HeapPageWriteTest
ant runtest -Dtest=HeapFileWriteTest
ant runtest -Dtest=BufferPoolWriteTest
ant runtest -Dtest=InsertTest
ant runsystest -Dtest=InsertTest
ant runsystest -Dtest=DeleteTest
ant runsystest -Dtest=EvictionTest