# Parallel sorting on a network of machines

By: Bao Tran, Hung Vu, Loc Bui

Based on paper: https://people.eecs.berkeley.edu/~culler/papers/p243-arpaci-dusseau.pdf?fbclid=IwAR0SKq4-o0fXEa8-FfWIYISrGwcgeHpxMnCgtvcqOanZ06ooHkaFKXuVV5g

### Note:

Right now, in every node from 01 to 06, we have an executable of gensort that could be used to generate samples. Read usage here: http://www.ordinal.com/gensort.html
The file, along with some generated data files (10K, 1_million, 100_millions), is in ~/localdisk/parallel_sorting

---
## merge.cpp

Merges all record files in the given folder. Expects the record files to be sorted.

### Compile:

g++ -std=c++17 merge.cpp -o merge

### Run:

./merge [-o output_file] [-r read_buffer_size] [-w write_buffer_size] file1 [file2 file3 ...]

### Options:

-o: Sets output filename. Defaults to `merge_output.txt`.

-r: Sets read buffer size. Defaults to `10'000'000` (~10MB).

-w: Sets write buffer size. Defaults to `10'000'000` (~10MB).

### Note:

Actual read buffer size is guaranteed to not exceed, but can be a little lower than the given value. The given buffer size will be evenly split between the input files.

Actual write buffer size is guaranteed to not exceed, but can be a little lower than the given value.

---
## two_pass_parallel.cpp

Example run command: `mpic++ two_pass_parallel.cpp merge.cpp -o two_pass_parallel && mpirun -ppn 8 -host node01,node02,node03,node04,node05,node06 ./two_pass_parallel /localdisk/parallel_sorting/20_mils`

### In this case:
1. We running on 6 hosts from node01 to node06, specified after -host
2. We are running 8 MPI processes on each node, specified by -ppn
3. We are reading 20 millions (`/localdisk/parallel_sorting/20_mils`) records from each node (120 millions in total)
4. Other data files are in the same directory (ex: `/localdisk/parallel_sorting/5_mils`)

---
## one_pass_parallel.cpp

Example run command: `mpic++ one_pass_parallel.cpp -o one_pass_parallel && mpirun -ppn 8 -host node01,node02,node03,node04,node05,node06 ./one_pass_parallel /localdisk/parallel_sorting/20_mils`
