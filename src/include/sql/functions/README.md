# SQL Functions Directory

The classes in this directory contain implementations of SQL *functions*. SQL functions are aware of
SQL NULL values and their accompanying NULL semantics in the context of the functions they provide.

This is a fairly thin layer performing NULL checking logic before dispatching to core SQL operator
logic in `src/include/sql/operations`, or dispatching to one of the underlying SQL runtime types.

**TODO(pmenon)**: Functions don't return SQL values, but write into output parameters. This makes
the API very unintuitive. The initial reason was due to performance since `tpl::sql::Val` can be as
large as 24 bytes depending on which derived class we're dealing with. However, since most
light-weight functions are inlined, I'm not certain this optimization matters and simply returning
by value will be just about as fast. Look into this!