#include "cpplambda.h"

using namespace simpledb::proto;

static inline int parse_args(ExecArgs& args)
{
    size_t len;
    std::cin.read((char*) &len, sizeof(size_t));
    std::string buf(len, 0);
    std::cin.read(&buf[0], len);

    if (!args.ParseFromString(buf))
        return -1;

    return 0;
}

static inline int return_output(const ExecResponse& resp)
{
    size_t len = resp.ByteSize();
    std::string output(sizeof(size_t), 0);
    *((size_t*) &output[0]) = len;
    output.append(resp.SerializeAsString());

    std::cout.write(&output[0], output.length());

    return 0;
}

int main(int argc, char* argv[])
{
    (void) argc;
    (void) argv;

    GOOGLE_PROTOBUF_VERIFY_VERSION;
    ExecArgs args;
    ExecResponse resp;

    if (parse_args(args) < 0)
        return EXEC_INPUT_ERROR;

    try
    {
        lambda_exec(args, resp);
    }
    catch (std::exception& e)
    {
        return EXEC_EXCEPTION;
    }

    if (return_output(resp) < 0)
        return EXEC_OUTPUT_ERROR;

    google::protobuf::ShutdownProtobufLibrary();
    return EXEC_OK;
}
