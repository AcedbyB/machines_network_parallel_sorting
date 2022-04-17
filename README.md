# Parallel sorting on a network of machines

Based on paper: https://people.eecs.berkeley.edu/~culler/papers/p243-arpaci-dusseau.pdf?fbclid=IwAR0SKq4-o0fXEa8-FfWIYISrGwcgeHpxMnCgtvcqOanZ06ooHkaFKXuVV5g

Note:

Right now, in node01 and node02, we have an executable of gensort that could be used to generate samples. Read usage here: http://www.ordinal.com/gensort.html
The file, along with some generated data files (10K, 1_million, 100_millions), is in ~/localdisk/parallel_sorting


## merge.cpp

Merges all record files in the given folder. Expects the record files to be sorted. Outputs to `merge_output.txt`.

### Compile:

g++ -std=c++17 merge.cpp -o merge

### Run:

./merge <folder_to_be_merged>
