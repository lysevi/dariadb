#include <libdariadb/compression/base_compressor.h>

using namespace dariadb::compression;

BaseCompressor::BaseCompressor(const ByteBuffer_Ptr &bw_) : bw(bw_) {}
