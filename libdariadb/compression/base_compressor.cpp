#include "base_compressor.h"

using namespace dariadb::compression;

BaseCompressor::BaseCompressor(const BinaryBuffer &bw):_bw(bw) {
}