#include <mpi.h>
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
using namespace std;

unordered_map <string, string> key_to_record;   // map key to each records
char* com_buffer; int* com_number; // communication_buffer for each process
vector<string> sending_to[256]; // records pending to be sent to other processes
vector<string> keys;    // keys that we will sort on this process
int num_procs, proc_id;	

const int NUM_NODES = 1;
const int RECORD_LENGTH = 99;


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


/* Function for writing to the destination file, running by the first processor of each machine/node */
int write_to_dst(string dst_file_name) {
    ofstream dst_file(dst_file_name);
    if (!dst_file.is_open()) {
        cout << "Unable to open destination file";
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
        cout<<"please supply paths to source and destination files (only)"<<endl;
        return -1;
    }

    MPI_Init(NULL, NULL);
    // Get world rank and size of processes
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &proc_id);
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


    // communication phase
    for(int i = 0; i < num_procs; i++) {
        if(proc_id == i) {
            for(int j = 0; j < num_procs; j++) {
                if(i == j) continue;

                // first, send the number of records we would send to this other rank
                int sending_num = sending_to[j].size();
                com_number[0] = sending_num;
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
            MPI_Recv(com_number, 1, MPI_INTEGER, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            int sending_num = com_number[0];
            cout<<proc_id<< " receiving from "<<i<<" "<<sending_num<< " records " << endl;

            // receive the actual records
            while(sending_num > 0) {
                sending_num--;
                MPI_Recv(com_buffer, RECORD_LENGTH, MPI_CHAR, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // after receiving, push this record to our local memory 
                string line = "";
                for(int index = 0; index < RECORD_LENGTH; index++) line += com_buffer[index];
                string key = line.substr(0, 10);
                keys.push_back(key);
                key_to_record[key] = line;
            }
        }
    }

    //  sorting steps, subject to change
    cout<<"SORT PHASE, "<<proc_id<< " SORTING TOTAL "<<keys.size()<< " records " << endl;
    sort(keys.begin(), keys.end());

    //  gathering phase, each process will send all of its data to the first processor of each machine
    for(int i = 1; i < num_procs_per_node; i++) {
        if(proc_id % num_procs_per_node == i) {
            // first, send the number of records we are holding
            int receiver = proc_id - proc_id%num_procs_per_node; // the first processor in our node
            int sending_num = keys.size();
            com_number[0] = sending_num;
            MPI_Send(com_number, 1, MPI_INTEGER, receiver, 1, MPI_COMM_WORLD);

            // now send the actual records
            for(string key: keys) {
                string record = key_to_record[key];
                for(int index = 0; index < RECORD_LENGTH; index++) com_buffer[index] = record[index];
                MPI_Send(com_buffer, RECORD_LENGTH, MPI_CHAR, receiver, 1, MPI_COMM_WORLD);
            }
        }
        else if (proc_id % num_procs_per_node == 0) {
            // receive the number of records incoming
            MPI_Recv(com_number, 1, MPI_INTEGER, proc_id + i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            int sending_num = com_number[0];
            cout<<proc_id<< " gathering from "<<i<<" "<<sending_num<< " records " << endl;

            // receive the actual records
            while(sending_num > 0) {
                sending_num--;
                MPI_Recv(com_buffer, RECORD_LENGTH, MPI_CHAR, proc_id + i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // after receiving, push this record to our local memory 
                string line = "";
                for(int index = 0; index < RECORD_LENGTH; index++) line += com_buffer[index];
                string key = line.substr(0, 10);
                keys.push_back(key);
                key_to_record[key] = line;
            }
        }
    }

    //  write to the destination file, again should be done only by the first processor of each machine
    if(proc_id % num_procs_per_node == 0) {
        if(write_to_dst(argv[2]) == -1) return -1;
    }

    // Finalize the MPI environment.
    MPI_Finalize();
}