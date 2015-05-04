Relation Manager
================
This module provides a library for managing database relations. This document 
gives a summary of the implementation. 

The relation manager (RM) is built on top of a record based file manager (RBFM).
Clients can gain access to the RM and make calls to create tables, insert tuples
into a table, update tuples, and drop tables. These calls are received by the RM,
which then verifies that the data is correct (i.e. the table exists, and the tuple
adheres to the proper schema). Afterwards, program execution is passed on to the
RBFM. The RBFM handles the logical memory management of a record based file. 

Each table in the database has its own corresponding file, which is saved as
tablename.tbl, where tablename is the user defined name of the table. The RM also
maintains two tables, which comprise the database catalog. These are the tables
"Tables" and "Columns". When using the RM to modify an existing table, the RM
scans the "Tables" and "Columns" relations to gain access to the physical data
of the relevant table. Once a filehandle to the table's file is obtained, the
RM can use the RBFM to make the necessary modifications.