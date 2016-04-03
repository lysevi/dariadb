#include "chunk.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

std::unique_ptr<ChunkPool> ChunkPool::_instance=nullptr;

ChunkPool::ChunkPool(){

}

ChunkPool::~ChunkPool(){
    for(auto p:_ptrs){
        :: operator delete(p);
    }
}

void ChunkPool::start(){

}

void ChunkPool::stop(){
    ChunkPool::_instance=nullptr;
}

ChunkPool*ChunkPool::instance(){
    if(_instance==nullptr){
        _instance=std::unique_ptr<ChunkPool>{new ChunkPool};
    }
    return _instance.get();
}

size_t ChunkPool::polled(){
    return _ptrs.size();
}

void* ChunkPool::alloc(std::size_t sz){
    std::lock_guard<std::mutex> lg(_mutex);
    if(this->_ptrs.size()!=0){
        auto result= this->_ptrs.back();
        this->_ptrs.pop_back();
        return result;
    }
    return ::operator new(sz);
}

void ChunkPool::free(void* ptr, std::size_t sz){
    std::lock_guard<std::mutex> lg(_mutex);
    this->_ptrs.push_front(ptr);
}

void* Chunk::operator new(std::size_t sz){
    return ChunkPool::instance()->alloc(sz);
}

void Chunk::operator delete(void* ptr, std::size_t sz){
    ChunkPool::instance()->free(ptr,sz);
}

Chunk::Chunk(const ChunkIndexInfo&index, const uint8_t* buffer, const size_t buffer_length) :
	_buffer_t(buffer_length),
	_mutex{}
{
	count = index.count;
	first = index.first;
	flag_bloom = index.flag_bloom;
	last = index.last;
	maxTime = index.maxTime;
	minTime = index.minTime;
	bw_pos = index.bw_pos;
	bw_bit_num = index.bw_bit_num;
	is_readonly = index.is_readonly;

	for (size_t i = 0; i < buffer_length; i++) {
		_buffer_t[i] = buffer[i];
	}

	range = Range{ _buffer_t.data(),_buffer_t.data() + buffer_length - 1 };
	bw = std::make_shared<BinaryBuffer>(range);
	bw->set_bitnum(bw_bit_num);
	bw->set_pos(bw_pos);
	
	c_writer = compression::CopmressedWriter(bw);
    c_writer.restore_position(index.writer_position);

	minTime = std::min(minTime, first.time);
	maxTime = std::max(maxTime, first.time);
}

Chunk::Chunk(size_t size, Meas first_m) :
	_buffer_t(size),
	_mutex()
{
	is_readonly = false;

	count = 0;
	first = first_m;
	last = first_m;
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
    flag_bloom = dariadb::storage::bloom_empty<dariadb::Flag>();
}

Chunk::~Chunk() {

}
bool Chunk::append(const Meas&m)
{
	assert(!is_readonly);

	std::lock_guard<std::mutex> lg(_mutex);
	auto t_f = this->c_writer.append(m);
	bw_pos = uint32_t(bw->pos());
	bw_bit_num= bw->bitnum();
    writer_position=c_writer.get_position();

	if (!t_f) {
		is_readonly = true;
		assert(c_writer.is_full());
		return false;
	}
	else {
		count++;

		minTime = std::min(minTime, m.time);
		maxTime = std::max(maxTime, m.time);
        flag_bloom = dariadb::storage::bloom_add(flag_bloom, m.flag);
		last = m;
		return true;
	}
}

bool Chunk::check_flag(const Flag& f) {
	if (f != 0) {
        if (!dariadb::storage::bloom_check(flag_bloom, f)) {
			return false;
		}
	}
	return true;
}
