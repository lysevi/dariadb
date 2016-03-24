#include "chunk.h"
#include "../bloom_filter.h"
#include <algorithm>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

Chunk::Chunk(size_t size, Meas first_m) :
	_buffer_t(size),
	_mutex()
{
	count = 0;
	first = first_m;

	minTime = std::numeric_limits<Time>::max();
	maxTime = std::numeric_limits<Time>::min();

	std::fill(_buffer_t.begin(), _buffer_t.end(), 0);

	using compression::BinaryBuffer;
	range = Range{ _buffer_t.data(),_buffer_t.data() + size - 1 };
	bw = std::make_shared<BinaryBuffer>(range);

	c_writer = compression::CopmressedWriter(bw);
	c_writer.append(first);
	minTime = std::min(minTime, first_m.time);
	maxTime = std::max(maxTime, first_m.time);
	flag_bloom = dariadb::bloom_empty<dariadb::Flag>();
}

Chunk::~Chunk() {

}
bool Chunk::append(const Meas&m)
{
	std::lock_guard<std::mutex> lg(_mutex);
	auto t_f = this->c_writer.append(m);

	if (!t_f) {
		return false;
	}
	else {
		count++;

		minTime = std::min(minTime, m.time);
		maxTime = std::max(maxTime, m.time);
		flag_bloom = dariadb::bloom_add(flag_bloom, m.flag);
		last = m;
		return true;
	}
}

bool Chunk::check_flag(const Flag& f) {
	if (f != 0) {
		if (!dariadb::bloom_check(flag_bloom, f)) {
			return false;
		}
	}
	return true;
}