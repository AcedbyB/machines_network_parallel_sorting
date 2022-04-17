#include <stdio.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <algorithm>
#include <assert.h>

#define UNLIKELY(x)     __builtin_expect((x), 0)

using namespace std;
int buffer_size { 10'000'000 };
int write_buffer_line_count { 10'000 };  // 10,000 lines is ~1MB (at ~100B per line)

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cout << "Invalid number of arguments. Usage: " << argv[0] << " <data_folder>\n";
        exit(1);
    }

    // Get all filepaths in given folder
    cout << "--- READ FOLDER ---" << endl;
    cout << "data folder: " << argv[1] << "\n";
    vector<string> filepaths;
    for (const auto &entry : filesystem::directory_iterator(argv[1])) {
        filepaths.push_back(entry.path());
    }

    // Create output file
    cout << "--- CREATE EMPTY OUTPUT FILE ---" << endl;


    // Initialize file readers
    cout << "--- INIT ---" << endl;
    vector<istream*> files;
    vector<filebuf*> fbs;
    vector<char*> lines;
    vector<bool> file_valid_flags;

    for (int i = 0; i < filepaths.size(); i++) {
        cout << filepaths[i] << endl;

        // init file streams
        fbs.push_back(new filebuf);
        fbs[i]->pubsetbuf(new char[buffer_size], buffer_size);
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
            cout << lines[i] << endl;
            file_valid_flags[i] = true;
        }
    }

    // Merge
    cout << "--- MERGE ---" << endl;
    string write_buffer;
    ofstream output_file ("merge_output.txt", ios::trunc);  // will overwrite existing output file

    while (any_of(file_valid_flags.begin(), file_valid_flags.end(), [](bool x) { return x; })) {
        int min_index { -1 };
        for (int i = 0; i < files.size(); i++) {
            if (file_valid_flags[i] && (min_index == -1 || string (lines[i]) < string (lines[min_index]))) {
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
            output_file.write(write_buffer.c_str(), write_buffer.size());
            write_buffer = "";
        }
    }

    output_file.write(write_buffer.c_str(), write_buffer.size());  // write remaining buffer to output file
}
