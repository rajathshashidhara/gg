#include <thread>
#include <mutex>
#include <atomic>
#include <fstream>
#include <streambuf>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

#include "net/simpledb.hh"
#include "net/socket.hh"
#include "protobufs/netformats.pb.h"
#include "util/exception.hh"
#include "util/file_descriptor.hh"
#include "util/crc16.hh"

using namespace std;
using namespace simpledb::proto;

static inline void send_request(TCPSocket& socket, KVRequest& req)
{
    std::string req_s = req.SerializeAsString();
    size_t len = req_s.length();
    static size_t slen = (ssize_t) sizeof(size_t);

    socket.write(string((char*) &len, slen));
    socket.write(req_s);
}

static inline bool receive_response(TCPSocket& socket, KVResponse& resp)
{
    size_t len;
    const static size_t slen = sizeof(size_t);

    len = *((size_t*) (&socket.read_exactly(slen)[0]));

    if (!resp.ParseFromString(socket.read_exactly(len)))
        return false;

    return true;
}

void SimpleDB::upload_files(
        const std::vector<storage::PutRequest> & upload_requests,
        const std::function<void(const storage::PutRequest&)>& success_callback)
{
    const size_t bucket_count = config_.num_;
    const size_t thread_count = min(upload_requests.size(), bucket_count);
    const size_t batch_size = config_.max_batch_size;

    vector<vector<storage::PutRequest>> buckets(bucket_count);
    vector<mutex> bucket_locks(bucket_count);
    atomic<size_t> barrier {thread_count};

    vector<thread> threads;
    for (size_t thread_index = 0;
        thread_index < thread_count;
        thread_index++)
    {
        threads.emplace_back(
            [&](const size_t index,
                    vector<vector<storage::PutRequest>>& buckets,
                    vector<mutex>& bucket_locks,
                    atomic<size_t>& barrier)
            {
                const size_t start_file_idx = index * (upload_requests.size()/thread_count);
                size_t end_file_idx = (index + 1) * (upload_requests.size()/thread_count);

                if (index == (min(thread_count, upload_requests.size()) - 1))
                {
                    end_file_idx = upload_requests.size();
                }

                for (size_t file_id = start_file_idx;
                    file_id < end_file_idx;
                    file_id += 1)
                {
                    const string & object_key =
                        upload_requests.at(file_id).object_key;

                    const auto idx = crc16(object_key) % bucket_count;
                    bucket_locks[idx].lock();
                    buckets[idx].emplace_back(move(upload_requests.at(file_id)));
                    bucket_locks[idx].unlock();
                }

                TCPSocket conn;
                conn.connect(config_.address_[index]);

                barrier.fetch_add(-1);
                while (barrier != 0) { this_thread::yield(); }

                if (index == 0)
                {
                    std::cerr << "Upload=" << upload_requests.size();
                    for (size_t tidx = 0; tidx < thread_count; tidx++)
                    {
                        std::cerr << " bIdx" << tidx << "=" << buckets[tidx].size();
                    }
                    std::cerr << std::endl;
                }

                size_t proc_idx = bucket_count;
                size_t non_empty = 0;
                for (size_t idx = 0; idx < bucket_count; idx++)
                {
                    if (buckets[idx].size() == 0)
                        continue;

                    if (non_empty == index)
                    {
                        proc_idx = idx;
                        break;
                    }
                    non_empty++;
                }

                std::cerr << "tIdx"<<tidx <<"=" << proc_idx << std::endl;
                if (proc_idx == bucket_count)
                    return;

                for (size_t first_file_idx = 0;
                        first_file_idx < buckets[proc_idx].size();
                        first_file_idx += batch_size)
                {
                    size_t expected_responses = 0;

                    for (size_t file_id = first_file_idx;
                        file_id < min(buckets[proc_idx].size(),
                        first_file_idx + batch_size);
                        file_id += 1)
                    {
                        const string & filename =
                            buckets[proc_idx].at(file_id).filename.string();
                        const string & object_key =
                            buckets[proc_idx].at(file_id).object_key;

                        string contents;
                        FileDescriptor file {
                            CheckSystemCall("open " + filename,
                                open(filename.c_str(), O_RDONLY))
                        };
                        while (not file.eof())
                            { contents.append(file.read()); }
                        file.close();

                        KVRequest req;
                        req.set_id(file_id);
                        auto put = req.mutable_put_request();
                        put->set_val(move(contents));
                        put->set_key(object_key);
                        put->set_immutable(false);
                        put->set_executable(false);

                        send_request(conn, req);
                        expected_responses++;
                    }

                    size_t response_count = 0;

                    while (response_count != expected_responses)
                    {
                        KVResponse resp;

                        if (!receive_response(conn, resp) ||
                                resp.return_code() != 0)
                            throw runtime_error("failed to get response");

                        const size_t response_index = resp.id();
                        success_callback(buckets[proc_idx][response_index]);

                        response_count++;
                    }
                }
            },
            thread_index, ref(buckets), ref(bucket_locks), ref(barrier)
        );
    }

    for (auto & thread : threads)
        thread.join();
}

void SimpleDB::download_files(
        const vector<storage::GetRequest>& download_requests,
        const function<void(const storage::GetRequest&)>& success_callback)
{
    const size_t bucket_count = config_.num_;
    const size_t thread_count = min(download_requests.size(), bucket_count);
    const size_t batch_size = config_.max_batch_size;

    vector<vector<storage::GetRequest>> buckets(bucket_count);
    vector<mutex> bucket_locks(bucket_count);
    atomic<size_t> barrier {thread_count};

    vector<thread> threads;
    for (size_t thread_index = 0;
        thread_index < thread_count;
        thread_index++)
    {
        threads.emplace_back(
            [&](const size_t index,
                    vector<vector<storage::GetRequest>>& buckets,
                    vector<mutex>& bucket_locks,
                    atomic<size_t>& barrier)
            {

                const size_t start_file_idx = index * (download_requests.size()/thread_count);
                size_t end_file_idx = (index + 1) * (download_requests.size()/thread_count);

                if (index == (min(thread_count, download_requests.size()) - 1))
                {
                    end_file_idx = download_requests.size();
                }

                for (size_t file_id = start_file_idx;
                    file_id < end_file_idx;
                    file_id += 1)
                {
                    const string & object_key =
                        download_requests.at(file_id).object_key;

                    const auto idx = crc16(object_key) % bucket_count;
                    bucket_locks[idx].lock();
                    buckets[idx].emplace_back(move(download_requests.at(file_id)));
                    bucket_locks[idx].unlock();
                }

                TCPSocket conn;
                conn.connect(config_.address_[index]);

                barrier.fetch_add(-1);
                while (barrier != 0) { this_thread::yield(); }

                if (index == 0)
                {
                    std::cerr << "Download=" << download_requests.size();
                    for (size_t tidx = 0; tidx < thread_count; tidx++)
                    {
                        std::cerr << " tIdx" << tidx << "=" << buckets[tidx].size();
                    }
                    std::cerr << std::endl;
                }

                size_t proc_idx = bucket_count;
                size_t non_empty = 0;
                for (size_t idx = 0; idx < bucket_count; idx++)
                {
                    if (buckets[idx].size() == 0)
                        continue;

                    if (non_empty == index)
                    {
                        proc_idx = idx;
                        break;
                    }
                    non_empty++;
                }

                std::cerr << "tIdx"<<tidx <<"=" << proc_idx << std::endl;
                if (proc_idx == bucket_count)
                    return;

                for (size_t first_file_idx = 0;
                        first_file_idx < buckets[proc_idx].size();
                        first_file_idx += batch_size)
                {
                    size_t expected_responses = 0;

                    for (size_t file_id = first_file_idx;
                        file_id < min(buckets[proc_idx].size(),
                        first_file_idx + batch_size);
                        file_id += 1)
                    {
                        const string & object_key =
                            buckets[proc_idx].at(file_id).object_key;

                        KVRequest req;
                        req.set_id(file_id);
                        auto put = req.mutable_get_request();
                        put->set_key(object_key);

                        send_request(conn, req);
                        expected_responses++;
                    }

                    size_t response_count = 0;

                    while (response_count != expected_responses)
                    {
                        KVResponse resp;

                        if (!receive_response(conn, resp) ||
                                resp.return_code() != 0)
                            throw runtime_error("failed to get response");

                        const size_t response_index = resp.id();
                        const string & filename =
                            buckets[proc_idx].at(response_index).filename.string();

                        roost::atomic_create(resp.val(), filename,
                            buckets[proc_idx][response_index].mode.initialized(),
                            buckets[proc_idx][response_index].mode.get_or(0));

                        success_callback(buckets[proc_idx][response_index]);

                        response_count++;
                    }
                }
            },
            thread_index, ref(buckets), ref(bucket_locks), ref(barrier)
        );
    }

    for (auto & thread : threads)
        thread.join();
}
