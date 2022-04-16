#include <stdio.h>
#include <string>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>

using namespace std;

int main(int argc, char** argv) {
    if(argc != 3) {
        cout<<"please supply paths to source and destination files (only)"<<endl;
        return -1;
    }

    ifstream src_file (argv[1]);
    if (!src_file.is_open()) {
        cout << "Unable to open source file" << endl;
        return -1;
    }

    ofstream dst_file (argv[2]);
    if (!dst_file.is_open()) {
        cout << "Unable to open destination file";
        return -1;
    }
    
    vector<string> keys;
    unordered_map <string, string> key_to_record;   // map key to each records

    // open and read in records from the source file
    string line;
    while ( getline (src_file, line) ) {
        string key = line.substr(0, 10);
        keys.push_back(key);
        key_to_record[key] = line;
    }
    src_file.close();

    // sort step, subject to change
    sort(keys.begin(), keys.end());

    // write to the destination file
    for(string key: keys) {
        dst_file << key_to_record[key];
        dst_file << '\n';
    }
    dst_file.close();

}