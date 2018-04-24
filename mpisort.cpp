/*
 * mpisort.cpp
 *
 *  Created on: Mar 12, 2018
 *      Author: mu
 */
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <time.h>
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <array>
#include <vector>
using namespace std;
int binarySearch(vector<int> arr, int l, int r, int x)
{
	if (r>=l)
	{
		int mid = l + (r - l)/2;
		if (arr[mid] == x){
			int search_pos = mid;
			while( arr[search_pos] <= x){
				if(search_pos <= arr.size()-1){
					search_pos += 1;
				}
				else{
					return search_pos;
				}

			}
			return search_pos;
		}

		if (arr[mid] > x)
			return binarySearch(arr, l, mid-1, x);
		return binarySearch(arr, mid+1, r, x);
	}
	else{
		int search_pos = l;
		while( arr[search_pos] <= x){
			if(search_pos <= arr.size()-1){
				search_pos += 1;
			}
			else{
				return search_pos;
			}

		}
		return search_pos;
	}


}
int findPos(vector<int> arr, int key)
{
	int l = 0, h = 1;
	int val = arr[0];

	while (val < key)
	{
		l = h;
		h = 2*h;
		val = arr[h];
	}

	return binarySearch(arr, l, h, key);
}

int main(int argc, char** argv){
	int world_rank;
	int world_size;

	ifstream file("test.txt");
	file.seekg(0,ios::end);
	int file_length = file.tellg();
	MPI_Init(&argc, &argv);
	//get the rank for this processor
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	//get the world size
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	long beginning = world_rank * (file_length / world_size);
	long end = (world_rank != world_size - 1) ? beginning + (file_length / world_size) : file_length;
	string line;
	if (beginning > 0){
		file.seekg(beginning - 1);
		if(file.get() != '\n'){
			getline(file,line);
		}
	}
	else{
		file.seekg(beginning);
	}
	int sum;
	string output = "";
	// creating the IntBuffer
	// use vector instead of malloc

	vector<int> IntBuffer;
	const int n = end - beginning;
	string newline;
	MPI_Barrier(MPI_COMM_WORLD);
	int i = 0;

	while (file.tellg() < end){
		getline(file,newline);
		if(newline.empty()) {
			break;
		}
		IntBuffer.push_back(stoi(newline));
		if (stoi(newline) == 0){
			cout << "no";
			cout << newline;
		}

		i++;
		output += newline + " ";
	}

	file.close();
	for(int i = 0; i < world_size; i++){
		MPI_Barrier(MPI_COMM_WORLD);
		if(i == 	world_rank){
			cout << "Rank" << world_rank << ": " << IntBuffer.size() << endl;
			cout << "Rank" << world_rank << ":  "<< output.length()<<"\n";
			cout << "Rank" << world_rank << ": " << "Beginning: " << beginning << " End: " << end << "\n";
		}
	}


	//	mergeSort(IntBuffer,0, end - beginning);
	sort(IntBuffer.begin(),IntBuffer.end());
	//	sort(IntBuffer.begin(), IntBuffer.end());
	//	for(int i = 1; i < world_size; i++){
	//		MPI_Barrier(MPI_COMM_WORLD);
	//		if(i == 	world_rank){
	//			cout << "Input from Rank" << world_rank << ":\n";
	//			for(int i = 0; i < IntBuffer.size(); i ++){
	//				cout << IntBuffer[i] << " ";
	//			}
	//
	//			cout << "\n";
	//		}
	//	}

	// Splitting the set into local splitters
	vector<int> local_splitters_vector;
	int component_size = IntBuffer.size() / world_size;
	for(int i = 1; i <= world_size - 1; i++){
		local_splitters_vector.push_back(IntBuffer[i*component_size]);
	}

	for(int i = 0; i < world_size; i++){
		MPI_Barrier(MPI_COMM_WORLD);
		if(i == world_rank){
			cout << "Splitters from Rank" << world_rank << ":\n";
			for(int i = 0; i < local_splitters_vector.size(); i ++){
				cout << local_splitters_vector[i] << " ";
			}

			cout << "\n";
		}
	}

	/*
	int local_splitters[local_splitters_vector.size()];
	for(int i = 0; i < local_splitters_vector.size(); i++){
		local_splitters[i] = local_splitters_vector[i];
	}
	 */
	int local_splitters_size = world_size * (world_size - 1);
	int* local_splitters_agg = (int*) malloc(sizeof(int) * local_splitters_size);
	MPI_Allgather(local_splitters_vector.data(),world_size - 1,MPI_INT,local_splitters_agg,world_size - 1,MPI_INT,MPI_COMM_WORLD);
	for (int i = 0; i < world_size;i ++){
		MPI_Barrier(MPI_COMM_WORLD);
		if (i == world_rank){
			cout << "Local Splitters Aggregate from Rank"<<"world_rank" <<":\n";
			for(int i = 0; i < local_splitters_size; i++) {

				cout << local_splitters_agg[i] << endl;


			}
		}

	}


	vector<int> global_splitters;
	for (int i = 0; i < world_size - 1; i++){
		int sum = 0;
		for(int j = 0; j < world_size; j++){
			sum += local_splitters_agg[i + j * (world_size - 1)  ] ;
			//			cout << local_splitters_agg[i + j * (world_size - 1)  ] ;
			//			cout << "\n";
		}
		double avg = sum/world_size;
		global_splitters.push_back(avg);
	}

	//	for (int i = 0; i < world_size;i ++){
	//		MPI_Barrier(MPI_COMM_WORLD);
	//		if (i == world_rank){
	//			cout << "Global Splitters from Rank"<<"world_rank" <<":\n";
	//			for(int j = 0; j < world_size-1; j++) {
	//				cout << global_splitters[j] << endl;
	//
	//			}
	//		}
	//	}
	//	vector<int> split_points;
	//	for (int i = 0; i < global_splitters.size();i++){
	//		if(findPos(IntBuffer,global_splitters[i]) != -1){
	//			cout << findPos(IntBuffer,global_splitters[i]);
	////			split_points.push_back(findPos(IntBuffer,global_splitters[i]));
	//		}
	//
	//	}

	for (int i = 0; i < world_size;i ++){
		MPI_Barrier(MPI_COMM_WORLD);
		if (i == world_rank){
			cout << "Splitter Indices of rank:"<< world_rank <<"\n";
			for(int j = 0; j < world_size-1; j++) {
				int index = findPos(IntBuffer,global_splitters[0]);
				cout << "First global splitter:" << global_splitters[0];
				cout << " " << IntBuffer[index - 1] << " " << IntBuffer[index] << " " << IntBuffer[index + 1];

				cout << "\n";

			}
		}
	}

	//	for(int i = 0; i < global_splitters.size(); i++) {
	//		cout << global_splitters[i];

	//	}
	//	cout << "\n";

	//TODO: Use all gather to get all the sizes
	//	int index = 0;
	//	for(int i = 0; i < world_size - 1; i++){
	//		int current = IntBuffer[0];
	//		while(current < global_splitters[i]){
	//
	//		}
	//	}
	//TODO: USE alltoall to send values in between splitters

	//	if(world_rank != 0){
	//		int current_key = IntBuffer[0];
	//		int divide_index = 0;
	//		while (current_key < global_splitters[0]){
	//			current_key = IntBuffer[divide_index];
	//			divide_index ++;
	//		}
	//		vector<int>::const_iterator first = IntBuffer.begin();
	//		vector<int>::const_iterator last = IntBuffer.begin() + divide_index ;
	//		vector<int> buffer_segment(first, last);
	//		int* size = (int*) malloc(sizeof(int) * 1);
	//		size[0] = buffer_segment.size();
	//		int* sizes = (int*) malloc(sizeof(int) * world_size);
	//		MPI_Send(size,1,MPI_INT,0,0,MPI_COMM_WORLD);
	//		MPI_Barrier(MPI_COMM_WORLD);
	//		// TODO: know which node this data is coming from
	//		// WHat is MPI_Status?
	//		// All to all
	//		MPI_Recv(sizes,1,MPI_INT,0,MPI_COMM_WORLD);
	//		MPI_Send(buffer_segment.data(),divide_index+1, MPI_INT,0,0,MPI_COMM_WORLD);
	//
	//	}
	//	for(int i = 0; i < world_size; i++){
	//		MPI_Barrier(MPI_COMM_WORLD);
	//		if(i == world_rank){
	//			cout << "Sizes from Rank" << world_rank << ":\n";
	//			for(int i = 0; i < local_splitters_vector.size(); i ++){
	//				cout << local_splitters_vector[i] << " ";
	//			}
	//
	//			cout << "\n";
	//		}
	//	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();



	return 0;
}



