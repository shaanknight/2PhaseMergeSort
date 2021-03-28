Two Phase Merge Sort (Non-threaded) :
-sequential.cpp
-compile instruction : g++ -O3 -march=native sequential.cpp
-run instruction : ./a.out <input_file> <output_file> <memory_limit> <asc/desc> <col_name> ...

Two Phase Merge Sort (Multithreaded) :
-parallel.cpp
-compile instruction : g++ -O3 -march=native -pthread parallel.cpp
-run instruction : ./a.out <input_file> <output_file> <memory_limit> <threads_count> <asc/desc> <col_name> ...