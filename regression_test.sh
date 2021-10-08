#!/bin/bash

# lab1
ant runtest -Dtest=TupleTest
ant runtest -Dtest=TupleDescTest
ant runtest -Dtest=CatalogTest
ant runtest -Dtest=HeapPageIdTest
ant runtest -Dtest=HeapPageTest
ant runtest -Dtest=HeapPageReadTest
ant runtest -Dtest=RecordIdTest
ant runtest -Dtest=HeapFileReadTest
