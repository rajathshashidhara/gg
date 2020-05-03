#ifndef SIMPLEDB_CPP_LAMBDA
#define SIMPLEDB_CPP_LAMBDA

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>

#include "protobufs/execformats.pb.h"

enum ExecErrorCodes {
    EXEC_OK,
    EXEC_INPUT_ERROR,
    EXEC_EXCEPTION,
    EXEC_OUTPUT_ERROR
};

extern void lambda_exec(const simpledb::proto::ExecArgs& params,
                simpledb::proto::ExecResponse& resp);

#endif /* SIMPLEDB_CPP_LAMBDA */