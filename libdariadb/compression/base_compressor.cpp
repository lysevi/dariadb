#include "base_compressor.h"

using namespace dariadb::compression;

BaseCompressor::BaseCompressor(const BinaryBuffer_Ptr &bw):_bw(bw) {
}
