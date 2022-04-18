#include <stdio.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <unistd.h>
#include <iterator>

using namespace std;

// #define DEBUG
#define UNLIKELY(x)     __builtin_expect((x), 0)

int read_buffer_size { 10'000'000 };
int write_buffer_size { 10'000'000 };
string output_filename = "merge_output.txt";

int main(int argc, char* argv[]) {
    // Parse options
    extern char* optarg;
    extern int optind;
    int opt;
    while ((opt = getopt(argc, argv, "o:r:w:")) != -1) {
        switch (opt) {
            case 'o':
                output_filename = string(optarg);
#ifdef DEBUG
                cout << "[Option] output file: " << output_filename << endl;
#endif
                break;
            case 'r':
                read_buffer_size = atoi(optarg);
                if (read_buffer_size == 0) {
                    cerr << "Invalid read buffer size. Exiting..." << endl;
                    exit(1);
                }
#ifdef DEBUG
                cout << "[Option] read buffer size: " << read_buffer_size << endl;
#endif
                break;
            case 'w':
                write_buffer_size = atoi(optarg);
                if (write_buffer_size == 0) {
                    cerr << "Invalid write buffer size. Exiting..." << endl;
                    exit(1);
                }
#ifdef DEBUG
                cout << "[Option] write buffer size: " << write_buffer_size << endl;
#endif
                break;
        }
    }

    // Check input files
#ifdef DEBUG
    cout << "\n--- READ INPUT FILES ---" << endl;
#endif

    if (optind == argc) {
        cerr << "No file given to merge. \
Usage: ./merge [-o output_file] [-r read_buffer_size] [-w write_buffer_size] file1 [file2 file3 ...]. \
Exiting..." << endl;
        exit(1);
    }

    vector<string> filepaths;
    for (int i = optind; i < argc; i++) {
        filepaths.push_back(string(argv[i]));
    }

    // Initialize file readers
    int read_buffer_size_per_file = read_buffer_size / filepaths.size();
#ifdef DEBUG
    cout << "\n--- INIT ---" << endl;
    cout << "read buffer size per file: " << read_buffer_size_per_file << endl;
    cout << "filepaths: ";
    copy(filepaths.begin(), filepaths.end(), ostream_iterator<string>(cout, ", "));
    cout << endl;
#endif
    if (read_buffer_size_per_file == 0) {
        cerr << "Not enough buffer size for each input file. Exiting..." << endl;
        exit(1);
    }

    vector<istream*> files;
    vector<filebuf*> fbs;
    vector<char*> lines;
    vector<bool> file_valid_flags;

    for (int i = 0; i < filepaths.size(); i++) {
#ifdef DEBUG
        cout << "initializing file: " << filepaths[i] << endl;
#endif
        // init file streams
        fbs.push_back(new filebuf);
        fbs[i]->pubsetbuf(new char[read_buffer_size_per_file], read_buffer_size_per_file);
        if (fbs[i]->open(filepaths[i], ios::in)) {
            istream* is { new istream (fbs[i]) };
            files.push_back(is);
        } else {
            cerr << "Cannot read " << filepaths[i] << ". Exiting..." << endl;
            exit(1);
        }

        // init line buffers, load 1st line in
        lines.push_back(new char[100]);
        file_valid_flags.push_back(false);
        if (files[i]->getline(lines[i], 100)) {
#ifdef DEBUG
            cout << "1st line: \"" << lines[i] << "\"" << endl;
#endif
            file_valid_flags[i] = true;
        }
    }

    // Merge
#ifdef DEBUG
    cout << "\n--- MERGE ---" << endl;
#endif

    string write_buffer;
    int write_buffer_line_count = write_buffer_size / 100;
#ifdef DEBUG
    cout << "write buffer line count: " << write_buffer_line_count << endl;
#endif
    ofstream output(output_filename, ios::trunc);  // will overwrite existing output file

    while (any_of(file_valid_flags.begin(), file_valid_flags.end(), [](bool x) { return x; })) {
        int min_index { -1 };
        for (int i = 0; i < files.size(); i++) {
            if (file_valid_flags[i]
                && (min_index == -1 || string (lines[i]) < string(lines[min_index]))) {
                min_index = i;
            }
        }

        if (UNLIKELY(min_index == -1)) { break; }  // break if no min_index found, unlikely to happen

        // write min line to write_buffer, update current line
        write_buffer += lines[min_index];
        write_buffer += '\n';
        file_valid_flags[min_index] = false;
        if (files[min_index]->getline(lines[min_index], 100)) {
            file_valid_flags[min_index] = true;
        }

        if (write_buffer.size() >= write_buffer_line_count) {
            // write to result file
            output.write(write_buffer.c_str(), write_buffer.size());
            write_buffer = "";
        }
    }

    output.write(write_buffer.c_str(), write_buffer.size());  // write remaining buffer to output file

#ifdef DEBUG
    cout << "\n--- DONE ---" << endl;
#endif
}
