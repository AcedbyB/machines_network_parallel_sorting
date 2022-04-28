#include <mpi.h>
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <sys/time.h>

using namespace std;

unordered_map <string, string> key_to_record;   // map key to each records
char* com_buffer; int* com_number; // communication_buffer for each process
vector<string> sending_to[256]; // records pending to be sent to other processes
vector<string> keys;    // keys that we will sort on this process
int tmp_file_count = 0;   // number of temporary runs this process has created
int num_procs, proc_id;
ifstream src_file;   // our source file object
bool eof_flag = false; // flag if we have reached the end of file

// timer variables
struct timeval start;
struct timeval finishSort;
struct timeval finishAll;
long compTime, sortTime;
double Time;

const int NUM_NODES = 6;      // how many nodes we have physically
const int BUFFER_LIMIT = 200000;   // limit of how many records we load into memory
const int RECORD_LENGTH = 99;
const string LOCALDISK_DIRECTORY = "/localdisk/parallel_sorting/";


/* Allocate the needed arrays */
void allocate_memory() {
	com_buffer = new char[100];
    com_number = new int[1];
}


/* Function for reading in from memory - into the first processor of each machine/node */
void read_into_memory() {
    // the range of keys for each processor
    int range_of_keys = 95/num_procs;

    // open and read in records from the source file
    string line;
    int records_cnt = 0;    // keeping track of how many records we have read in
    while ( getline (src_file, line) ) {
        records_cnt++;
        // deciding the rank/proc_id that this record should be sent to, based on the first characters (8 bytes)
        int dst_rank = (((int)line[0]) - 33)/range_of_keys;     // first 33 characters of ASCII is not used in our keys
        if(dst_rank != proc_id) {
            sending_to[dst_rank].push_back(line);
        }
        else {
            string key = line.substr(0, 10);
            keys.push_back(key);
            key_to_record[key] = line;
        }

        if(records_cnt == BUFFER_LIMIT) break;
    }

    // don't close it yet since we will need to resume the src_file object
    if(records_cnt > 0) return;

    // if last read we read nothing => we reached the end and should close the file
    src_file.close();
    eof_flag = 1;
}


/* Function for writing a run to disk for later merge, each process write to its corresponding proc_id */
int write_to_disk(int run) {
    ofstream dst_file(LOCALDISK_DIRECTORY + to_string(proc_id) + "." + to_string(run));
    if (!dst_file.is_open()) {
        cout << "Unable to open destination file in proc: " << proc_id <<endl;
        return -1;
    }

    for(string key: keys) {
        dst_file << key_to_record[key];
        dst_file << '\n';   // have to add in 1 end line character since getline neglect this
    }
    dst_file.close();
}


/* Function clearing out all buffers/memory */
void clear_all_memory() {
    key_to_record.clear();
    keys.clear();
    for(int j = 0; j < num_procs; j++) {
        sending_to[j].clear();
    }
}

int main(int argc, char** argv) {
    if(argc != 2) {
        cout<<"please supply paths to source only"<<endl;
        return -1;
    }

    MPI_Init(NULL, NULL);

    // Get world rank and size of processes
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);

    // start timer
    if(proc_id == 0) gettimeofday(&start, 0);

    // Get the name of the processor
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);

    //  allocating communication memory/buffer
    allocate_memory();

    // Print off a hello world message
    printf("Processor rank %d out of %d processors, running on machine %s \n",
           proc_id, num_procs, processor_name);

    // Calculating the number of processes per node. In this case we have 6 machines - 6 nodes (01 - 06)
    int num_procs_per_node = num_procs/NUM_NODES;

    // open the targeting file, return if something is wrong
    src_file.open (argv[1], ifstream::in);
    if (!src_file.is_open()) {
        cout << "Unable to open source file in proc: " << proc_id << endl;
        return -1;
    }


    int runs = 0;
    while(!eof_flag) {
        // handling input issue, should be run by only the first processor of each machine
        // read in BUFFER_LIMIT records on each run
        if(proc_id % num_procs_per_node == 0) {
            read_into_memory();

            // if we reached eof
            if(eof_flag) com_number[0] = 1; // don't go
            else com_number[0] = 0; // go next run

            for(int j = proc_id + 1; j < proc_id + num_procs_per_node; j++)
                MPI_Send(com_number, 1, MPI_INTEGER, j, 1, MPI_COMM_WORLD);
        }
        else {
            // Wait to receive the go or no go signal
            MPI_Recv(com_number, 1, MPI_INTEGER, proc_id - proc_id%num_procs_per_node, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            eof_flag = com_number[0];
        }

        MPI_Barrier( MPI_COMM_WORLD );

        // return if we no longer need to process
        if(eof_flag) break;

        // what run is this?
        runs++;

        // communication phase
        for(int i = 0; i < NUM_NODES; i++) {
            // if we are the first processor of our current node
            if(proc_id == num_procs_per_node*i) {
                for(int j = 0; j < num_procs; j++) {
                    if(j == proc_id) continue;

                    // first, send the number of records we would send to this other rank
                    int sending_num = sending_to[j].size();
                    com_number[0] = sending_num;
                    // cout<<proc_id<< " sending to "<<j<<" "<<sending_num<< " records " << endl;
                    MPI_Send(com_number, 1, MPI_INTEGER, j, 1, MPI_COMM_WORLD);

                    // now we will send the actual records
                    for(string record: sending_to[j]) {
                        for(int index = 0; index < RECORD_LENGTH; index++) com_buffer[index] = record[index];
                        MPI_Send(com_buffer, RECORD_LENGTH, MPI_CHAR, j, 1, MPI_COMM_WORLD);
                    }
                }
            }
            else {
                // receive the number of records incoming
                MPI_Recv(com_number, 1, MPI_INTEGER, i*num_procs_per_node, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                int sending_num = com_number[0];
                // cout<<proc_id<< " receiving from "<<i*num_procs_per_node<<" "<<sending_num<< " records " << endl;

                // receive the actual records
                while(sending_num > 0) {
                    sending_num--;
                    MPI_Recv(com_buffer, RECORD_LENGTH, MPI_CHAR, i*num_procs_per_node, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    // after receiving, push this record to our local memory
                    string line(&com_buffer[0], &com_buffer[0] + RECORD_LENGTH);
                    string key(&com_buffer[0], &com_buffer[0] + 10);
                    keys.push_back(key);
                    key_to_record[key] = line;
                }
            }
        }

        // clear buffer memory
        for(int j = 0; j < num_procs; j++) {
            sending_to[j].clear();
        }
    }

    // Sort all records in memory
    sort(keys.begin(), keys.end());
    if(proc_id == 0) gettimeofday(&finishSort, 0);  // track the time it takes to sort

    // Write final result
    ofstream dst_file(LOCALDISK_DIRECTORY + to_string(proc_id));
    if (!dst_file.is_open()) {
        cout << "Unable to open destination file in proc: " << proc_id <<endl;
        return -1;
    }

    for(string key: keys) {
        dst_file << key_to_record[key];
        dst_file << '\n';   // have to add in 1 end line character since getline neglect this
    }
    dst_file.close();

    // Finalize the MPI environment.
    MPI_Finalize();

    // calculating and printing end timer
    if(proc_id == 0) {
        gettimeofday(&finishAll, 0);  // end timer

        sortTime = (finishSort.tv_sec - start.tv_sec) * 1000000;
        sortTime = sortTime + (finishSort.tv_usec - start.tv_usec);
        double formattedSortTime = (double)sortTime;
        // printf("Sort time: %f Secs\n",(double)formattedSortTime/1000000.0);

        compTime = (finishAll.tv_sec - start.tv_sec) * 1000000;
        compTime = compTime + (finishAll.tv_usec - start.tv_usec);
        Time = (double)compTime;
        double formattedMergeTime = (double) (Time - formattedSortTime);
        // printf("Merge time: %f Secs\n",(double)formattedMergeTime/1000000.0);
        printf("Total time: %f Secs\n",(double)Time/1000000.0);

    }
}
