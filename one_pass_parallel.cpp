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
int num_procs, proc_id;	
// timer variables
struct timeval start;
struct timeval finish;
long compTime;
double Time;

const int NUM_NODES = 6;
const int RECORD_LENGTH = 99;
const string LOCALDISK_DIRECTORY = "/localdisk/parallel_sorting/";


/* Allocate the needed arrays */
void allocate_memory() {
	com_buffer = new char[100];
    com_number = new int[1];
}


/* Function for reading in from memory into the first processor of each machine/node */
int read_into_memory(string src_file_name) {
    ifstream src_file (src_file_name);
    if (!src_file.is_open()) {
        cout << "Unable to open source file" << endl;
        return -1;
    }

    int range_of_keys = 97/num_procs;   // the range of keys for each processor 

    // open and read in records from the source file
    string line;
    while ( getline (src_file, line) ) {
        // deciding the rank that this should be sent to, based on the first characters (8 bytes)
        int dst_rank = (((int)line[0]) - 31)/range_of_keys;     // first 31 characters of ASCII is not used in our keys
        if(dst_rank != proc_id) {
            sending_to[dst_rank].push_back(line);
        }
        else {
            string key = line.substr(0, 10);
            keys.push_back(key);
            key_to_record[key] = line;
        }
    }
    src_file.close();
    return 0;
}


/* Function for writing to the destination file(s), each process write to is corresponding proc_id */
int write_to_dst(string directory_name) {
    ofstream dst_file(LOCALDISK_DIRECTORY + to_string(proc_id));
    if (!dst_file.is_open()) {
        cout << "Unable to open destination file"<<endl;
        return -1;
    }

    for(string key: keys) {
        dst_file << key_to_record[key];
        dst_file << '\n';   // have to add in 1 end line character since getline neglect this
    }
    dst_file.close();
}


int main(int argc, char** argv) {
    if(argc != 3) {
        cout<<"please supply paths to source and destination folder (only)"<<endl;
        return -1;
    }

    MPI_Init(NULL, NULL);
    // Get world rank and size of processes
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);

    if(proc_id == 0) gettimeofday(&start, 0); // start timer

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

    // handling input issue, should be run by only the first processor of each machine
    if(proc_id % num_procs_per_node == 0) {
        if(read_into_memory(argv[1]) == -1) return -1;
    }

    // wait for all processes to be done reading
    MPI_Barrier(MPI_COMM_WORLD);

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
                    sending_num--;
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

    //  sorting steps, subject to change
    cout<<"SORT PHASE, "<<proc_id<< " SORTING TOTAL "<<keys.size()<< " records " << endl;
    sort(keys.begin(), keys.end());

    //  write to the destination file, each process writes to its own file
    if(write_to_dst(argv[2]) == -1) return -1;

    // Finalize the MPI environment.
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();

    // calculating and printing end timer
    if(proc_id == 0) {
        gettimeofday(&finish, 0);  // end timer

        compTime = (finish.tv_sec - start.tv_sec) * 1000000;
        compTime = compTime + (finish.tv_usec - start.tv_usec);
        Time = (double)compTime;

        printf("Application time: %f Secs\n",(double)Time/1000000.0);
    }
}