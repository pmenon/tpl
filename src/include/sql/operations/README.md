# SQL Operators Directory

This directory contains implementations of common SQL *operators* such as comparisons, arithmetic,
bitwise, and casting operators. SQL operators are NOT aware of SQL NULL semantics. That logic must 
be handled at a higher-level: either by SQL functions in `src/include/sql/functions` (used by 
tuple-at-a-time code) or in vector-based operations in `src/include/sql/vector_operations`. In a
sense, operators sit beneath tuple-at-a-time and vector-at-a-time functions.

Because operator logic is shared between tuple-at-a-time code and vectorized code, they are 
implemented as C++ function objects. Tuple-at-a-time code requires the implementation of the
operator to exist at compile time to avoid function call overhead on a per-tuple basis. Vectorized
code requires function objects because it leverages templates and template meta-programming for
compile-time code generation.

It is of tantamount important that all operators use this function object design. For "heavy"
functions whose implementation complexity outweigh invocation overhead (e.g., SQL LIKE(*)), you may
implement that logic in a CPP file, but it must still be wrapped in a function object in order to be
used in vectorized code. 

1. All SQL operators require a function object implementation here.
2. SQL NULL semantic logic is implemented by wrapping these operators in SQL functions or vector-
   based operators in `src/include/sql/vector_operations`.
   
While this requires some work, there are plenty of examples to use. And ... it's really not that
much work.