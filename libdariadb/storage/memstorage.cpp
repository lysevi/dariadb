#include "memstorage.h"
#include "../utils/utils.h"
#include "../compression.h"
#include "../flags.h"
#include "subscribe.h"
#include "chunk.h"
#include "../timeutil.h"
#include "inner_readers.h"

#include <limits>
#include <algorithm>
#include <map>
#include <tuple>
#include <assert.h>
#include <mutex>
#include <atomic>

using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

typedef std::map<Id, ChuncksList> ChunkMap;

class MemoryStorage::Private
{
public:
	Private(size_t size) :
		_size(size),
		_min_time(std::numeric_limits<dariadb::Time>::max()),
		_max_time(std::numeric_limits<dariadb::Time>::min()),
		_subscribe_notify(new SubscribeNotificator),
		_chunks_count(0)
	{
		_subscribe_notify->start();
		
	}

	~Private() {
		_subscribe_notify->stop();
		_chuncks.clear();
	}

	Time minTime() { return _min_time; }
	Time maxTime() { return _max_time; }

	Chunk_Ptr getFreeChunk(dariadb::Id id) {
		Chunk_Ptr resulted_chunk = nullptr;
		auto ch_iter = _free_chunks.find(id);
		if (ch_iter != _free_chunks.end()) {
			if (!ch_iter->second->is_full()) {
				return ch_iter->second;
			}
		}
		return resulted_chunk;
	}

	Chunk_Ptr make_chunk(dariadb::Meas first) {
		auto ptr = new Chunk(_size, first);
		auto chunk = Chunk_Ptr{ ptr };
        this->_chuncks.push_back(chunk);
		this->_free_chunks[first.id] = chunk;
		_chunks_count++;
		return chunk;
	}

	append_result append(const Meas& value) {
		std::lock_guard<std::mutex> lg(_mutex);

		Chunk_Ptr chunk = this->getFreeChunk(value.id);

		if (chunk == nullptr) {
			chunk=make_chunk(value);
		}
		else {
            //std::cout<<"append new "<<chunk->count<< " chunks: "<<this->chunks_total_size()<<std::endl;
			if (!chunk->append(value)) {
				assert(chunk->is_full());
				chunk = make_chunk(value);
			}
		}
		
		assert(chunk->last.time == value.time);

		_min_time = std::min(_min_time, value.time);
		_max_time = std::max(_max_time, value.time);

		_subscribe_notify->on_append(value);

		return dariadb::append_result(1, 0);
	}


	append_result append(const Meas::PMeas begin, const size_t size) {
		dariadb::append_result result{};
		for (size_t i = 0; i < size; i++) {
			result = result + append(begin[i]);
		}
		return result;

	}

    size_t size()const { return _size; }
	size_t chunks_size()const { return _chuncks.size(); }

	size_t chunks_total_size()const {
		return _chunks_count.load();
	}

	void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
		std::lock_guard<std::mutex> lg(_subscribe_mutex);
		auto new_s = std::make_shared<SubscribeInfo>(ids, flag, clbk);
		_subscribe_notify->add(new_s);
	}

	Reader_ptr currentValue(const IdArray&ids, const Flag& flag) {
        std::lock_guard<std::mutex> lg(_mutex);
		auto res_raw = new InnerCurrentValuesReader();
		Reader_ptr res{ res_raw };
		for (auto &kv : _free_chunks) {
			auto l = kv.second->last;
			if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), l.id) == ids.end())) {
				continue;
			}
			if ((flag == 0) || (l.flag == flag)) {
				res_raw->_cur_values.push_back(l);
			}
		}
		return res;
	}

	dariadb::storage::ChuncksList drop_old_chunks(const dariadb::Time min_time) {
        std::lock_guard<std::mutex> lg(_mutex);
		ChuncksList result;
        auto now = dariadb::timeutil::current_time();

        for (auto& chunk : _chuncks) {
                auto past = (now - min_time);
                if ((chunk->maxTime < past)&&(chunk->is_full())) {
                    result.push_back(chunk);
                    chunk->is_dropped=true;
                    if (this->_free_chunks[chunk->first.id] == chunk) {
                        this->_free_chunks.erase(chunk->first.id);
                    }
                }
        }
		if (result.size() > size_t(0)){
            this->_chuncks.remove_if([](const Chunk_Ptr &c){return c->is_dropped;});
			update_max_min_after_drop();
		}
		_chunks_count= _chunks_count -long(result.size());
		return result;
	}
	
	//by memory limit
	ChuncksList drop_old_chunks_by_limit(const size_t max_limit) {
		std::lock_guard<std::mutex> lg(_mutex);
		ChuncksList result{};

		if (chunks_total_size() > max_limit) {

			int64_t iterations = (int64_t(chunks_total_size()) - (max_limit - size_t(max_limit / 3)));
			if (iterations < 0) {
				return result;
			}

            for (auto& chunk : _chuncks) {

                if (chunk->is_readonly) {
                    result.push_back(chunk);

                    if (this->_free_chunks[chunk->first.id] == chunk) {
                        this->_free_chunks.erase(chunk->first.id);
                    }
                    _chunks_count--;
                    chunk->is_dropped=true;
                }

                if (int64_t(result.size()) >= iterations) {
                    break;
                }
            }
			if (result.size() > size_t(0)){
				update_max_min_after_drop();
			}
            this->_chuncks.remove_if([](const Chunk_Ptr &c){return c->is_dropped;});
		}
		return result;
	}

	void update_max_min_after_drop() {
		this->_min_time = std::numeric_limits<dariadb::Time>::max();
		this->_max_time = std::numeric_limits<dariadb::Time>::min();
        for (auto& c : _chuncks) {

				_min_time = std::min(c->minTime, _min_time);
				_max_time = std::max(c->maxTime, _max_time);

		}
	}

	dariadb::storage::ChuncksList drop_all() {
        std::lock_guard<std::mutex> lg(_mutex);
		ChuncksList result;
		
        for (auto& chunk : _chuncks) {
				result.push_back(chunk);
		}
		this->_free_chunks.clear();
		this->_chuncks.clear();
		_chunks_count = 0;
		//update min max
		this->_min_time = std::numeric_limits<dariadb::Time>::max();
		this->_max_time = std::numeric_limits<dariadb::Time>::min();
		
		return result;
	}

	ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
		std::lock_guard<std::mutex> lg(_mutex);
		ChuncksList result{};

        for (auto ch : _chuncks) {
            if ((utils::inInterval(from, to, ch->minTime)) || (utils::inInterval(from, to, ch->maxTime))) {
                continue;
            }

            if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), ch->first.id) == ids.end())) {
                continue;
            }

            if (flag != 0) {
                if (!ch->check_flag(flag)) {
                    continue;
                }
            }
            result.push_back(ch);

        }
		return result;
	}

	IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
		IdToChunkMap result;
        for (auto cur_chunk : _chuncks) {
            if (cur_chunk->minTime > timePoint){
                break;
            }
            if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), cur_chunk->first.id) == ids.end())) {
				continue;
            }

            if (!cur_chunk->check_flag(flag)) {
                continue;
            }
            if (cur_chunk->minTime <= timePoint) {
                result[cur_chunk->first.id] = cur_chunk;
                break;
            }

		}
		return result;
	}

	dariadb::IdArray getIds()const {
		dariadb::IdArray result;
		result.resize(_chuncks.size());
		size_t pos = 0;
        for (auto&kv : _free_chunks) {
			result[pos] = kv.first;
			pos++;
		}
		return result;
	}

	void add_chunks(const ChuncksList&clist) {
		for (auto c : clist) {
            _chuncks.push_back(c);
			auto search_res = _free_chunks.find(c->first.id);
			if (search_res == _free_chunks.end()) {
				_free_chunks[c->first.id] = c;
			}
			else {
				assert(false);
			}
		}
	}
protected:
	size_t _size;

    ChuncksList _chuncks;
	IdToChunkMap _free_chunks;
	Time _min_time, _max_time;
	std::unique_ptr<SubscribeNotificator> _subscribe_notify;
	mutable std::mutex _subscribe_mutex;
	mutable std::mutex _mutex;
	std::atomic<int64_t> _chunks_count;
};


MemoryStorage::MemoryStorage(size_t size)
	:_Impl(new MemoryStorage::Private(size)) {
}


MemoryStorage::~MemoryStorage() {
}

Time MemoryStorage::minTime() {
	return _Impl->minTime();
}

Time MemoryStorage::maxTime() {
	return _Impl->maxTime();
}

append_result MemoryStorage::append(const dariadb::Meas &value) {
	return _Impl->append(value);
}

append_result MemoryStorage::append(const dariadb::Meas::PMeas begin, const size_t size) {
	return _Impl->append(begin, size);
}

size_t  MemoryStorage::size()const {
	return _Impl->size();
}

size_t  MemoryStorage::chunks_size()const {
	return _Impl->chunks_size();
}

size_t MemoryStorage::chunks_total_size()const {
	return _Impl->chunks_total_size();
}

void MemoryStorage::subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
	return _Impl->subscribe(ids, flag, clbk);
}

Reader_ptr MemoryStorage::currentValue(const IdArray&ids, const Flag& flag) {
	return  _Impl->currentValue(ids, flag);
}

void MemoryStorage::flush()
{
}

dariadb::storage::ChuncksList MemoryStorage::drop_old_chunks(const dariadb::Time min_time){
	return _Impl->drop_old_chunks(min_time);
}

dariadb::storage::ChuncksList MemoryStorage::drop_old_chunks_by_limit(const size_t max_limit) {
	return _Impl->drop_old_chunks_by_limit(max_limit);
}
dariadb::storage::ChuncksList MemoryStorage::drop_all()
{
	return _Impl->drop_all();
}

ChuncksList MemoryStorage::chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
	return _Impl->chunksByIterval(ids, flag, from, to);
}

IdToChunkMap MemoryStorage::chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
	return _Impl->chunksBeforeTimePoint(ids, flag, timePoint);
}

dariadb::IdArray MemoryStorage::getIds()const {
	return _Impl->getIds();
}

void MemoryStorage::add_chunks(const ChuncksList&clist) {
	return _Impl->add_chunks(clist);
}
