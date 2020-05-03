#include <iostream>
#include "protobuf_stream_parser.hh"
#include "protobufs/netformats.pb.h"

using namespace std;
using namespace gg::protobuf;

template <class Message>
bool ProtobufStreamParser<Message>::parsing_step()
{
    Message resp;
    string lenbuf;

    switch (state)
    {
    case READ_LEN_PENDING:
        if (buffer_.size() < sizeof(in_progress_message_len))
            return false;
        
        lenbuf = buffer_.get_and_pop_bytes(sizeof(in_progress_message_len));
        in_progress_message_len = *((size_t*) &lenbuf[0]);
        state = READ_PAYLOAD_PENDING;

        return true;
    
    case READ_PAYLOAD_PENDING:
        if (buffer_.size() < in_progress_message_len)
            return false;

        if (!resp.ParseFromString(buffer_.get_and_pop_bytes(in_progress_message_len)))
            throw runtime_error("Failed to parse ExecutionResponse");

        complete_messages_.emplace(move(resp));
        in_progress_message_len = 0;
        state = READ_LEN_PENDING;

        return true;

    default:
        throw runtime_error("Invalid parse state");
    }
}

template <class Message>
void ProtobufStreamParser<Message>::parse(const string& buf)
{
    /* append buf to internal buffer */
    buffer_.append( buf );

    /* parse as much as we can */
    while ( parsing_step() ) {}   
}

template class
ProtobufStreamParser<gg::protobuf::ExecutionResponse>;
template class
ProtobufStreamParser<simpledb::proto::KVResponse>;