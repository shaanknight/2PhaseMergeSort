# 2PhaseMergeSort
## Sequential and Parallel Implementation using pthreads

Two-Phase, Multiway Merge-Sort is the preferred sorting algorithm in many database applications, where it is required to sort very large number of records and available main memory is quite limited. 

Briefly, this algorithm consists of:
- Phase 1: Sort main-memory-sized pieces of the data, so every record is
part of a sorted list that just fits in the available main memory. There
may thus be any number of these sorted subhsts, which we merge in the
next phase.
- Phase 2: Merge all the sorted sublists into a single sorted list.


## Implementation Specifications 

- The input contains a large numver of tuples with fixed number of attributes/columns.
- The metadata file will contain information about the size of the different columns (in bytes).
- The data type for all columns will be a string.
- Given a specific main memory constraint and number of threads as input (in case of parallel implementation), the program is capable of sorting in both asceding and descending order according to a given preference of columns.

## Compilation
Sequential Implementation (Non threaded) : 
```sh
g++ -O3 -march=native sequential.cpp
```
Parallel Implementation (Multithreaded) : 
```sh
g++ -O3 -march=native -pthread parallel.cpp
```

## Run Instructions
Sequential Implementation (Non threaded) : 
```sh
./a.out <input_file> <output_file> <memory_limit> <asc/desc> <col_name> ...
```
Parallel Implementation (Multithreaded) : 
```sh
./a.out <input_file> <output_file> <memory_limit> <threads_count> <asc/desc> <col_name> ...
```

... signifies if any other columns are to be provided in preference order.

## About gensort and valsort
- gensort : The gensort program can be used to generate input records for the sorting benchmarks.
- valsort : The valsort program can be used to validate the sort output file is correct. 

More information on the same can be found in [Gensort and Valsort](http://www.ordinal.com/gensort.html)

## Benchmarking
The benchmarking details of both the implementations can be found in analysis.pdf file. Note that since the benchmarking has been done on relatively medium sized files, bounded by size of 500 MB, there can hardly be any speedup seen, rather try increasing size of input in GBs to see the effect of parallelism.
