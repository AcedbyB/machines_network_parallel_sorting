#include <stdio.h>
#include <string>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstdio>
using namespace std;

vector<string> keys;
unordered_map <string, string> key_to_record;   // map key to each records
const int BUFFER_LIMIT = 1000000;   // limit of how many records we load into memory
vector<string> tmp_files; // array of temporary files (or runs)
int tmp_file_count = 0;   // number of temporary files this process has created
const string LOCALDISK_DIRECTORY = "/localdisk/parallel_sorting/";


/*sub-routine for sorting the current in-memory records and writing them to a run*/
int sort_and_write() {
    tmp_file_count++;   // increment the number of tmp files we current have
    ofstream run_file (LOCALDISK_DIRECTORY + to_string(tmp_file_count));  // use the number as the next run's name
    if (!run_file.is_open()) {
        cout << "Unable to create/open run file " << tmp_file_count << endl;
        return -1;
    }
    tmp_files.push_back(to_string(tmp_file_count));

    sort(keys.begin(), keys.end());
    // write to the destination file
    for(string key: keys) {
        run_file << key_to_record[key];
        run_file << '\n';
    }
    keys.clear();

    run_file.close();
}


/* Function for reading in from disk, once our buffer reached limit, we sort in-memory */
int read_and_process(string src_file_name) {
    ifstream src_file (src_file_name);
    if (!src_file.is_open()) {
        cout << "Unable to open source file" << endl;
        return -1;
    }
    // open and read in records from the source file
    string line;
    while ( getline (src_file, line) ) {
        string key = line.substr(0, 10);
        keys.push_back(key);
        key_to_record[key] = line;

        // if keys contain the maximum number of records, 
        // we call a sub-routine to sort, clear, and write the current sorted records to disk
        if(keys.size() == BUFFER_LIMIT) {
            if(sort_and_write() == -1) return -1;
        }
    }
    src_file.close();

    return 0;
}


/* clean-up sub-routine for the tmp files (runs) we made along the process */
void clean_up_tmps() {
    for(string tmp_file_name: tmp_files) {
        string place_holder = (LOCALDISK_DIRECTORY + tmp_file_name);
        char* removing_file = &place_holder[0];  // extra-step since remove only accept char*
        remove(removing_file);
    }
}


int main(int argc, char** argv) {
    if(argc != 3) {
        cout<<"please supply paths to source and destination files (only)"<<endl;
        return -1;
    }

    if(read_and_process(argv[1]) == -1) return -1;

    // run the merge process
    string place_holder = "./merge -o " + (string)argv[2];
    for(string tmp_file: tmp_files) {
        place_holder += " " + LOCALDISK_DIRECTORY + tmp_file;
    }
    char* merge_command = &place_holder[0];
    system(merge_command);

    clean_up_tmps();
}