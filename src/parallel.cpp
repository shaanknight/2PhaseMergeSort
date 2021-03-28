#include <bits/stdc++.h>
#include <thread> 
#include <ext/pb_ds/assoc_container.hpp>
#include <ext/pb_ds/tree_policy.hpp>
#include <chrono>
 
using namespace __gnu_pbds;
using namespace std;
 
#define ordered_set tree<int, null_type,less<int>, rb_tree_tag,tree_order_statistics_node_update>
// order_of_key (val): returns the no. of values less than val
// find_by_order (k): returns the kth largest element.(0-based)

mt19937 rng(chrono::steady_clock::now().time_since_epoch().count());
 
// for pair comparison function(ascending order) use return (i1.ff < i2.ff);
 
/* string operations :
   str.substr(x,y) : returns a substring str[x],str[x+1],...str[x+y-1]
   find("abc"): Searches the string for the first occurrence of the substring specified in arguments.
   str.substr(x) : returns a substring str[x],str[x+1],...str[str.size()-1]
*/

// compile instruction : g++ -O3 -march=native -pthread parallel.cpp

const int INF = 1e9;
const int MEMORY_CONST = 312500;
 
int column_count,each_record_size;
vector<pair<string,int> > columns;

string input_file,output_file,sorting_order;
int memory_limit,count_threads;
bool is_ascending;
vector<int> column_priorities;
map<string,int> column_mapping;

void __init()
{
	cout << "###start execution" << "\n";
	ifstream metadata("metadata.txt");
	string line;
    while(metadata >> line)
    {
        auto index_comma = line.find(",");
        if(index_comma == string::npos)
        {
        	cout << "Invalid Metadata format" << "\n";
        	exit(1);
        }
        string name = line.substr(0,index_comma);
        string length = line.substr(index_comma+1);
        columns.push_back(make_pair(name,stoi(length)));
        column_count += 1;
        column_mapping[name] = column_count;
        each_record_size += stoi(length);
    }
    metadata.close();
}

void parse_query(int argc, char **argv)
{
	if(argc < 7) 
	{
	    cerr << "Invalid query : The number of command-line arguments to the executable is low." << endl;
	    exit(1);
	}

	input_file = argv[1];
	output_file = argv[2];
	memory_limit = stoi(argv[3]);
	count_threads = stoi(argv[4]);
	sorting_order = argv[5];

	if(sorting_order == "asc")
		is_ascending = true;

	string considered_column;
	for(int i=6;i<argc;++i)
	{
		considered_column = argv[i];
		if(!column_mapping[considered_column])
		{
			cerr << "Invalid query : Column " << considered_column << " not found in metadata." << endl;
	    	exit(1);
		}
		column_priorities.push_back(column_mapping[considered_column]-1);
	}
}

vector<string> read_tuple(string line)
{
	vector<string> tuple;
	istringstream buffer(line);
	for(int i=0;i<column_count;++i)
	{
		char char_read;
		string entry;
		for(int j=0;j<columns[i].second;++j)
		{
			buffer.get(char_read);
			entry.push_back(char_read);
		}
		buffer.get(char_read);
		buffer.get(char_read);
		tuple.push_back(entry);
	}
	return tuple;
}

bool comparator(vector<string> &tuple1,vector<string> &tuple2)
{	
	for(auto ind:column_priorities)
	{
		if(tuple1[ind] != tuple2[ind])
		{
			if(is_ascending) return tuple1[ind] < tuple2[ind];
			else return tuple1[ind] > tuple2[ind];
		}
	}
	return 0;
}

void phase1sort_write(int chunk_index,vector<vector<string> > chunk)
{
	cout << "##sorting #" << to_string(chunk_index) << " sublist" << "\n";
	sort(chunk.begin(),chunk.end(),comparator);
	cout << "Writing to disk #" << to_string(chunk_index) << "\n";
	string intermediate_path = to_string(chunk_index)+"_intermediate_file.txt";
	ofstream intermediate_write(intermediate_path);
	for(auto &row : chunk)
	{
		for(auto &entry : row)
			intermediate_write << entry << "  ";
		intermediate_write << "\n";
	}
	intermediate_write.close();
}

int phase1_exceute()
{
	cout << "##running Phase-1" << "\n";
	queue<thread> threads;

	int max_chunk_size = (MEMORY_CONST * memory_limit) / (each_record_size*count_threads);
	vector<vector<string> > chunk;

	ifstream input(input_file);
	string line;
	int chunk_index = 0;
	while(getline(input, line, '\n'))
	{
		chunk.push_back(read_tuple(line));
		if((int)(chunk.size()) == max_chunk_size)
		{
			// 1 thread is the main program threads, so we have count_threads - 1 to use
			// so if the count of threads is saturated, we wait untill the 1st temporary threads 
			// gets its job done and hence we pop it out
			if(threads.size() == count_threads - 1) 
			{
	        	threads.front().join();
	        	threads.pop();
	        }
	        chunk_index++;
	        threads.push(thread(phase1sort_write,chunk_index,chunk));
	        chunk.clear();
		}
	}
	input.close();
	// there's some content on the chunk and hence this needs to be sorted
	// the main program thread would sort that part
	if(!chunk.empty())
	{
		chunk_index++;
		phase1sort_write(chunk_index,chunk);
		chunk.clear();
	}
	cout << "Number of sub-files (splits): " << chunk_index << endl;
	while(!threads.empty())
	{
		threads.front().join();
		threads.pop();
	}
	return chunk_index;
}

struct compare_tuple_phase2
{
	bool operator()(pair<vector<string>, int> &tuple1, pair<vector<string>, int> &tuple2) 
	{
    	for(auto ind:column_priorities)
		{
			if(tuple1.first[ind] != tuple2.first[ind])
			{
				if(is_ascending) return tuple1.first[ind] > tuple2.first[ind];
				else return tuple1.first[ind] < tuple2.first[ind];
			}
		}
		return 0;
	};
};

void phase2_execute(int chunks_count)
{
	cout << "##running Phase-2" << "\n";
	int max_batch_size = (MEMORY_CONST * memory_limit) / each_record_size;
	if(chunks_count >= max_batch_size)
	{
		cout << "Memory Usage Error : Too many chunks created" << "\n";
		exit(1);
	}
	vector<ifstream> chunk_streams;
	string line;
    priority_queue<pair<vector<string>, int>, vector<pair<vector<string>, int>>, compare_tuple_phase2> pq;
    for(int i=1;i<=chunks_count;++i)
    {
    	ifstream ifs(to_string(i)+"_intermediate_file.txt");
    	chunk_streams.push_back(std::move(ifs));
    	if(getline(chunk_streams[i-1],line,'\n'))
    		pq.push(make_pair(read_tuple(line),i));
    }
    ofstream output(output_file);
    cout << "Sorting..." << "\n";
    cout << "Writing to disk" << "\n";
    while(!(pq.empty()))
    {
    	auto next_chunk = pq.top();
    	pq.pop();
    	int column_index = 0;
    	for (auto &entry : next_chunk.first)
    	{
    		column_index++;
      		output << entry;
      		if(column_index<column_count)
      			output << "  ";
    	}
    	output << "\r\n";
    	if(getline(chunk_streams[next_chunk.second-1],line,'\n'))
    		pq.push(make_pair(read_tuple(line),next_chunk.second));
    }
    output.close();
    for(int i=1;i<=chunks_count;++i)
    {
    	chunk_streams[i-1].close();
    	string intermediate_path = to_string(i)+"_intermediate_file.txt";
    	char buffer[(int)intermediate_path.size()+1];
    	strcpy(buffer, intermediate_path.c_str());
    	std::remove(buffer);
    }
}

int main(int argc, char **argv)
{
	__init();
	parse_query(argc,argv);
	int chunks_count = phase1_exceute();
	phase2_execute(chunks_count);
	cout << "###completed execution" << "\n";
	return 0;
}