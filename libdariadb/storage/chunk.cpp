#include "chunk.h"
#include "bloom_filter.h"
#include <algorithm>
#include <cassert>
#include <cstring>

using namespace dariadb;
using namespace dariadb::utils;
using namespace dariadb::storage;
using namespace dariadb::compression;

std::unique_ptr<ChunkPool> ChunkPool::_instance=nullptr;

ChunkPool::ChunkPool(){
	_max_size = ChunkPool_default_max_size;
}

ChunkPool::~ChunkPool(){
	while (!_ptrs.empty()) {
		auto out = _ptrs.front();		
		_ptrs.pop();
		if(out!=nullptr){
			:: operator delete(out);
		}
	}
}

void ChunkPool::start(size_t max_size){
	if (max_size != 0) {
		ChunkPool::instance()->_max_size = max_size;
	}
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
	_locker.lock();
	auto result = _ptrs.size();
	_locker.unlock();
	return result;
}

void* ChunkPool::alloc(std::size_t sz) {
	void*result = nullptr;
	_locker.lock();
	if (!_ptrs.empty()) {
		result = _ptrs.front();
		_ptrs.pop();
	}
	_locker.unlock();
	if (result == nullptr) {
		result = ::operator new(sz);
	}	
	memset(result, 0, sz);
	return result;
}

void ChunkPool::free(void* ptr, std::size_t){
	_locker.lock();
    if (_ptrs.size() < _max_size) {
        _ptrs.push(ptr);
		_locker.unlock();
		return;
    }
    else {
		_locker.unlock();
		::operator delete(ptr);
	}
	
}

void* Chunk::operator new(std::size_t sz){
    return ChunkPool::instance()->alloc(sz);
}

void Chunk::operator delete(void* ptr, std::size_t sz){
    ChunkPool::instance()->free(ptr,sz);
}

Chunk::Chunk(const ChunkIndexInfo&index, const uint8_t* buffer, const size_t buffer_length) :
	_buffer_t(new u8vector(buffer_length)),
    _locker{}
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
	is_dropped = index.is_dropped;

	for (size_t i = 0; i < buffer_length; i++) {
		(*_buffer_t)[i] = buffer[i];
	}

	range = Range{ _buffer_t->data(),_buffer_t->data() + buffer_length };
	assert(size_t(range.end - range.begin)==buffer_length);
	bw = std::make_shared<BinaryBuffer>(range);
	bw->set_bitnum(bw_bit_num);
	bw->set_pos(bw_pos);
	
	c_writer = compression::CopmressedWriter(bw);
    c_writer.restore_position(index.writer_position);
}

Chunk::Chunk(size_t size, Meas first_m) :
	_buffer_t(new u8vector(size)),
    _locker()
{
	is_readonly = false;
    is_dropped=false;
	count = 0;
	first = first_m;
	last = first_m;
	minTime = std::numeric_limits<Time>::max();
	maxTime = std::numeric_limits<Time>::min();

	std::fill(_buffer_t->begin(), _buffer_t->end(), 0);

	using compression::BinaryBuffer;
	range = Range{ _buffer_t->data(),_buffer_t->data() + size };
	bw = std::make_shared<BinaryBuffer>(range);

	c_writer = compression::CopmressedWriter(bw);
	c_writer.append(first);
    minTime = first_m.time;
    maxTime = first_m.time;
    flag_bloom = dariadb::storage::bloom_empty<dariadb::Flag>();
}

Chunk::~Chunk() {
	this->bw = nullptr;
	_buffer_t->clear();
	delete _buffer_t;
}

bool Chunk::append(const Meas&m)
{
	if (is_dropped || is_readonly) {
		throw MAKE_EXCEPTION("(is_dropped || is_readonly)");
	}

    std::lock_guard<utils::Locker> lg(_locker);
	auto t_f = this->c_writer.append(m);
	writer_position = c_writer.get_position();

	if (!t_f) {
		is_readonly = true;
		assert(c_writer.is_full());
		return false;
	}
    else {
		bw_pos = uint32_t(bw->pos());
		bw_bit_num = bw->bitnum();

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
