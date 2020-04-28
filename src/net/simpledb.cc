#include <thread>
#include <fstream>
#include <streambuf>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

#include "net/simpledb.hh"
#include "protobufs/netformats.pb.h"
#include "util/exception.hh"
#include "util/file_descriptor.hh"

using namespace std;
using namespace simpledb::proto;

static inline bool send_request(int fd, KVRequest& req)
{
    std::string req_s = req.SerializeAsString();
    size_t len = req_s.length();
    static ssize_t slen = (ssize_t) sizeof(size_t);

    if (send(fd, &len, sizeof(size_t), 0) < slen)
        return false;

    if (send(fd, req_s.c_str(), len, 0) < (ssize_t)len)
        return false;

    return true;
}

bool receive_response(int fd, KVResponse& resp)
{
    size_t len;
    char* s;
    const static ssize_t slen = (ssize_t) sizeof(size_t);
    size_t offset;
    ssize_t ret;

    offset = 0;
    while (offset < slen)
    {
        if ((ret = recv(fd, ((char*) &len) + offset, slen - offset, 0)) < 0)
            return false;

        offset += ret;
    }

    s = new char[len];
    if (s == nullptr)
        return false;

    offset = 0;
    while (offset < len)
    {
        if ((ret = recv(fd, s + offset, len - offset, 0)) < 0)
            return false;

        offset += ret;
    }

    if (!resp.ParseFromArray(s, len))
        return false;

    delete s;
    return true;
}

void SimpleDB::upload_files(
        const std::vector<storage::PutRequest> & upload_requests,
        const std::function<void(const storage::PutRequest&)>& success_callback)
{
    const size_t thread_count = config_.max_threads;
    const size_t batch_size = config_.max_batch_size;

    vector<thread> threads;
    for (size_t thread_index = 0;
        thread_index < thread_count;
        thread_index++)
    {
        if (thread_index < upload_requests.size())
        {
            threads.emplace_back(
                [&](const size_t index)
                {
                    int fd = socket(AF_INET,
                                SOCK_STREAM, 0);
                    if (fd < 0)
                    {
                        throw runtime_error("could not create socket");
                    }

                    auto addr = config_.address_[0].to_sockaddr();
                    if (connect(fd,
                        &addr,
                        sizeof(struct sockaddr)) < 0)
                    {
                        throw runtime_error("error connecting to \
                                            simpledb server");
                    }

                    for (size_t first_file_idx = index;
                            first_file_idx < upload_requests.size();
                            first_file_idx += thread_count * batch_size)
                    {
                        size_t expected_responses = 0;

                        for (size_t file_id = first_file_idx;
                            file_id < min(upload_requests.size(),
                            first_file_idx + thread_count * batch_size);
                            file_id += thread_count)
                        {
                            const string & filename =
                                upload_requests.at(file_id).filename.string();
                            const string & object_key =
                                upload_requests.at(file_id).object_key;

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

                            if (!send_request(fd, req))
                                throw runtime_error("failed to send request");

                            expected_responses++;
                        }

                        size_t response_count = 0;

                        while (response_count != expected_responses)
                        {
                            KVResponse resp;

                            if (!receive_response(fd, resp) ||
                                    resp.return_code() != 0)
                                throw runtime_error("failed to get response");

                            const size_t response_index = resp.id();
                            success_callback(upload_requests[response_index]);

                            response_count++;
                        }
                    }

                    close(fd);
                },
                thread_index
            );
        }
    }

    for (auto & thread : threads)
        thread.join();
}

void SimpleDB::download_files(
        const vector<storage::GetRequest>& download_requests,
        const function<void(const storage::GetRequest&)>& success_callback)
{
    const size_t thread_count = config_.max_threads;
    const size_t batch_size = config_.max_batch_size;

    vector<thread> threads;
    for (size_t thread_index = 0;
        thread_index < thread_count;
        thread_index++)
    {
        if (thread_index < download_requests.size())
        {
            threads.emplace_back(
                [&](const size_t index)
                {
                    int fd = socket(AF_INET,
                                SOCK_STREAM, 0);
                    if (fd < 0)
                    {
                        throw runtime_error("could not create socket");
                    }

                    auto addr = config_.address_[0].to_sockaddr();
                    if (connect(fd,
                        &addr,
                        sizeof(struct sockaddr)) < 0)
                    {
                        throw runtime_error("error connecting to \
                                            simpledb server");
                    }

                    for (size_t first_file_idx = index;
                            first_file_idx < download_requests.size();
                            first_file_idx += thread_count * batch_size)
                    {
                        size_t expected_responses = 0;

                        for (size_t file_id = first_file_idx;
                            file_id < min(download_requests.size(),
                            first_file_idx + thread_count * batch_size);
                            file_id += thread_count)
                        {
                            const string & object_key =
                                download_requests.at(file_id).object_key;

                            KVRequest req;
                            req.set_id(file_id);
                            auto put = req.mutable_get_request();
                            put->set_key(object_key);

                            if (!send_request(fd, req))
                                throw runtime_error("failed to send request");

                            expected_responses++;
                        }

                        size_t response_count = 0;

                        while (response_count != expected_responses)
                        {
                            KVResponse resp;

                            if (!receive_response(fd, resp) ||
                                    resp.return_code() != 0)
                                throw runtime_error("failed to get response");

                            const size_t response_index = resp.id();
                            const string & filename =
                                download_requests.at(response_index).filename.string();

                            roost::atomic_create(resp.val(), filename,
                                download_requests[response_index].mode.initialized(),
                                download_requests[response_index].mode.get_or(0));

                            success_callback(download_requests[response_index]);

                            response_count++;
                        }
                    }

                    close(fd);
                },
                thread_index
            );
        }
    }

    for (auto & thread : threads)
        thread.join();
}
