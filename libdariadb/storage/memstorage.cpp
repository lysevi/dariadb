#include "memstorage.h"
#include "../utils/utils.h"
#include "../flags.h"
#include "../utils/locker.h"
#include "../utils/asyncworker.h"
#include "../timeutil.h"
#include "subscribe.h"
#include "chunk.h"
#include "cursor.h"
#include "inner_readers.h"
#include "chunk_by_time_map.h"
#include <limits>
#include <algorithm>
#include <assert.h>
#include <unordered_map>

#include <stx/btree_map>

using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

typedef std::map<Id, ChunksList> ChunkMap;
typedef ChunkByTimeMap<Chunk_Ptr, typename stx::btree_map<dariadb::Time, Chunk_Ptr>> ChunkWeaksMap;
typedef std::unordered_map<dariadb::Id, ChunkWeaksMap> MultiTree;

class MemstorageCursor : public Cursor {
public:
	ChunksList _chunks;
	ChunksList::iterator it;
	MemstorageCursor(ChunksList &chunks)
		:_chunks(chunks.begin(),chunks.end())
	{
		this->reset_pos();
	}
	~MemstorageCursor() {
		_chunks.clear();
	}

	bool is_end()const override {
		return it == _chunks.end();
	}

    void readNext(Cursor::Callback*cbk)  override {
		if (!is_end()) {
			cbk->call(*it);
			++it;
		}
		else {
			Chunk_Ptr empty;
			cbk->call(empty);
		}
	}

	void reset_pos() override {
		it = _chunks.begin();
	}
};

class MemoryStorage::Private: protected dariadb::utils::AsyncWorker<Chunk_Ptr>
{
public:
	Private(size_t size, MemoryStorage::Limits lmts) :
		_size(size),
		_min_time(std::numeric_limits<dariadb::Time>::max()),
		_max_time(std::numeric_limits<dariadb::Time>::min()),
		_subscribe_notify(new SubscribeNotificator),
		_limits(lmts)
	{
		_subscribe_notify->start();
		this->start_async();
        _cw=nullptr;
	}

	~Private() {
		this->stop_async();
		_subscribe_notify->stop();
		_chunks.clear();
        _multitree.clear();
	}

	Time minTime() { return _min_time; }
	Time maxTime() { return _max_time; }

    bool minMaxTime(dariadb::Id id, dariadb::Time*minResult, dariadb::Time*maxResult) {
        bool result = false;
        *minResult = std::numeric_limits<dariadb::Time>::max();
        *maxResult = std::numeric_limits<dariadb::Time>::min();
        {
            std::lock_guard<std::mutex> lg(_locker_chunks);
            auto mt_iter=_multitree.find(id);
            if(mt_iter!=_multitree.end()){
                if(mt_iter->second.size()!=0){
                    auto resf = mt_iter->second.begin();
                    auto rest = mt_iter->second.end();
                    rest--;
                    *minResult = resf->second->minTime;
                    *maxResult = rest->second->maxTime;
                    result = true;
                }
            }
        }
        {
            _locker_free_chunks.lock();
            auto it = _free_chunks.find(id);
            _locker_free_chunks.unlock();
            if (it != _free_chunks.end()) {
                *minResult = std::min(it->second->minTime, *minResult);
                *maxResult = std::max(it->second->maxTime, *maxResult);
                result = true;
            }
        }
        return result;
    }
	
	bool maxTime(dariadb::Id, dariadb::Time*){
		return 0;
	}
	
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
		this->_free_chunks[first.id] = chunk;
		return chunk;
	}

	append_result append(const Meas& value) {
        std::lock_guard<std::mutex> lg(_locker_free_chunks);

		Chunk_Ptr chunk = this->getFreeChunk(value.id);

		if (chunk == nullptr) {
			chunk=make_chunk(value);
		}
		else {
			if (!chunk->append(value)) {
                this->add_async_data(chunk);
				chunk = make_chunk(value);
			}
		}
		
		assert(chunk->last.time == value.time);
		{
			std::lock_guard<std::mutex> lg_minmax(_locker_min_max);
			_min_time = std::min(_min_time, value.time);
			_max_time = std::max(_max_time, value.time);
		}
		_subscribe_notify->on_append(value);

		return dariadb::append_result(1, 0);
	}

	//TODO _chunks.size() can be great than max_limit.
	void call_async(const Chunk_Ptr&chunk)override {
        std::lock_guard<std::mutex> lg_ch(_locker_chunks);
		this->_chunks.insert(std::make_pair(chunk->maxTime, chunk));

        auto mt_iter=_multitree.find(chunk->first.id);
        if(mt_iter!=_multitree.end()){
            mt_iter->second.insert(std::make_pair(chunk->maxTime, chunk));
        }else{
            _multitree[chunk->first.id].insert(std::make_pair(chunk->maxTime, chunk));
        }
		assert(chunk->is_full());
        if(_cw!=nullptr){
            _cw->append(chunk);
			if (_limits.max_mem_chunks == 0) {
				if (_limits.old_mem_chunks != 0) {
					auto old_chunks = drop_old_chunks(_limits.old_mem_chunks);
					_cw->append(old_chunks);
				}
			}
			else {
				auto old_chunks = drop_old_chunks_by_limit(_limits.max_mem_chunks);
				_cw->append(old_chunks);
			}
        }
	}

	void flush() {
		this->flush_async();
	}

	size_t queue_size()const {
		return this->async_queue_size();
	}

	append_result append(const Meas::PMeas begin, const size_t size) {
		dariadb::append_result result{};
		for (size_t i = 0; i < size; i++) {
			result = result + append(begin[i]);
		}
		return result;

	}

    size_t size()const { return _size; }
	size_t chunks_size()const { return _chunks.size()+_free_chunks.size(); }

	size_t chunks_total_size()const {
		return this->_chunks.size();
	}

	void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
        std::lock_guard<std::mutex> lg(_subscribe_locker);
		auto new_s = std::make_shared<SubscribeInfo>(ids, flag, clbk);
		_subscribe_notify->add(new_s);
	}

	Reader_ptr currentValue(const IdArray&ids, const Flag& flag) {
        std::lock_guard<std::mutex> lg(_locker_free_chunks);
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

	dariadb::storage::ChunksList drop_old_chunks(const dariadb::Time min_time) {
        std::unique_lock<std::mutex> lg_drop(_locker_drop,  std::defer_lock);
        std::unique_lock<std::mutex> lg_ch(_locker_chunks, std::defer_lock);
        std::lock(lg_drop,lg_ch);
		ChunksList result;
        auto now = dariadb::timeutil::current_time();

        for (auto& kv: _chunks) {
			auto chunk = kv.second;
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
            for(auto&kv:_multitree){
                kv.second.remove_droped();
            }
            _chunks.remove_droped();
            update_min_after_drop();
        }

		return result;
	}
	
	//by memory limit
	ChunksList drop_old_chunks_by_limit(const size_t max_limit) {
		ChunksList result{};

		if (chunks_total_size() >= max_limit) {
            std::lock_guard<std::mutex> lg_drop(_locker_drop);
			
            //std::unique_lock<std::mutex> lg_ch(_locker_chunks, std::defer_lock);
            //std::lock(lg_drop,lg_ch);

            int64_t iterations = (int64_t(chunks_total_size()) - (max_limit - size_t(max_limit / 3)));
			if (iterations < 0) {
				return result;
			}

            for (auto& kv : _chunks) {
				auto chunk = kv.second;
                assert(chunk!=nullptr);
                if (chunk->is_readonly) {
                    result.push_back(chunk);
                    chunk->is_dropped=true;
                }

                if (int64_t(result.size()) >= iterations) {
                    break;
                }
            }
			if (result.size() > size_t(0)) {
                for(auto&kv:_multitree){
                    kv.second.remove_droped();
                }

                _chunks.remove_droped();
                update_min_after_drop();
			}

		}
		return result;
	}

	void update_min_after_drop() {
		auto new_min = std::numeric_limits<dariadb::Time>::max();
        for (auto& kv : _chunks) {
            new_min = std::min(kv.second->minTime, new_min);
		}
		std::lock_guard<std::mutex> lg(_locker_min_max);
		_min_time = new_min;
	}

	dariadb::storage::ChunksList drop_all() {
        std::lock_guard<std::mutex> lg_drop(_locker_drop);
        std::lock_guard<std::mutex> lg_ch(_locker_chunks);
		ChunksList result;
		
        for (auto& kv : _chunks) {
            result.push_back(kv.second);
		}
		//drops after, becase page storage can be in 'overwrite mode'
		for (auto& kv : _free_chunks) {
			result.push_back(kv.second);
		}
		this->_free_chunks.clear();
		this->_chunks.clear();
        this->_multitree.clear();
		//update min max
		this->_min_time = std::numeric_limits<dariadb::Time>::max();
		this->_max_time = std::numeric_limits<dariadb::Time>::min();
		
		return result;
	}

    bool check_chunk_flag( Flag flag, const Chunk_Ptr&ch)
    {
        if ((flag == 0) || (!ch->check_flag(flag))) {
            return true;
        }
        return false;
    }

    bool check_chunk_to_qyery(const IdArray &ids, Flag flag, const Chunk_Ptr&ch)    {
        if ((ids.size() == 0) || (std::find(ids.begin(), ids.end(), ch->first.id) != ids.end())) {
            return check_chunk_flag(flag,ch);
        }
        return false;
    }

    bool check_chunk_to_interval(Time from, Time to, const Chunk_Ptr&ch)	{
		if ((utils::inInterval(from, to, ch->minTime)) || (utils::inInterval(from, to, ch->maxTime))) {
            return true;
		}
		return false;
	}

    Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
        
		ChunksList result{};

        IdArray id_a=ids;
        if(id_a.empty()){
            id_a=this->getIds();
        }
        for(auto i:id_a){
            _locker_chunks.lock();
            auto mt_iter=_multitree.find(i);
            if(mt_iter!=_multitree.end()){
                if(mt_iter->second.size()!=0){
                    auto resf =  mt_iter->second.get_lower_bound(from);
                    auto rest = mt_iter->second.get_upper_bound(to);

                    for(auto it=resf;it!=rest;++it){
                        auto ch=it->second;
                        if (ch->is_dropped) {
                            throw MAKE_EXCEPTION("MemStorage::ch->is_dropped");
                        }
                        if(ch->first.id!=i){
                            continue;
                        }
                        if ((check_chunk_flag(flag, ch)) && (check_chunk_to_interval(from, to, ch))){
                            result.push_back(ch);
                        }

                    }
                }
            }
            _locker_chunks.unlock();

            _locker_free_chunks.lock();
            auto fres=_free_chunks.find(i);
            _locker_free_chunks.unlock();

            if(fres!=_free_chunks.end()){
                if ((check_chunk_flag(flag, fres->second)) && (check_chunk_to_interval(from, to, fres->second))){
                    result.push_back(fres->second);
                }
            }
        }
		if (result.size() > (this->chunks_total_size()+ _free_chunks.size())) {
			throw MAKE_EXCEPTION("result.size() > this->chunksBeforeTimePoint()");
		}
		MemstorageCursor *raw = new MemstorageCursor{ result };
		return Cursor_ptr{raw};
	}

    IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
        IdToChunkMap result;

        IdArray id_a=ids;
        if(id_a.empty()){
            id_a=this->getIds();
        }
        for(auto i:id_a){
            {
                std::lock_guard<std::mutex> lg(_locker_free_chunks);
                auto fc_res=_free_chunks.find(i);
                if(fc_res!=_free_chunks.end()){
                    if (fc_res->second->minTime <= timePoint) {
                        result[fc_res->second->first.id] = fc_res->second;
                        continue;
                    }
                }
            }
            if (!_chunks.empty()) {
                std::lock_guard<std::mutex> lg(_locker_chunks);
                auto mt_res=_multitree.find(i);
                if(mt_res!=_multitree.end()){
                    if(mt_res->second.size()>size_t(0)){
                        auto rest = mt_res->second.get_upper_bound(timePoint);
                        auto resf= mt_res->second.begin();
                        if(rest!=mt_res->second.begin()){
                            resf=rest;
                            --resf;
                        }else{
                            rest=mt_res->second.end();
                        }
                        for (auto it = resf; it != rest; ++it) {
                            auto cur_chunk = it->second;

                            if (check_chunk_to_qyery(ids, flag, cur_chunk)) {
                                if (cur_chunk->minTime <= timePoint) {
                                    result[cur_chunk->first.id] = cur_chunk;
                                }
                            }
                            if (it == rest) {
                                break;
                            }

                        }
                    }
                }
            }
        }
        return result;
    }

	dariadb::IdArray getIds() {
		dariadb::IdArray result;
		result.resize(_free_chunks.size());
		size_t pos = 0;
        for (auto&kv : _free_chunks) {
			result[pos] = kv.first;
			pos++;
		}
        std::sort(result.begin(),result.end());
		return result;
	}

    bool append(const ChunksList&clist) {
		for (auto c : clist) {
           if(!this->append(c)){
               return false;
           }
		}
        return true;
	}

    bool append(const Chunk_Ptr&c) {
        std::make_pair(c->maxTime, c);
        auto search_res = _free_chunks.find(c->first.id);
        if (search_res == _free_chunks.end()) {
            _free_chunks[c->first.id] = c;
        }
        else {
            assert(false);
            return false;
        }
        return true;
    }

    void set_chunkWriter(ChunkWriter*cw){
        _cw=cw;
    }
protected:
	size_t _size;

    ChunkByTimeMap<Chunk_Ptr> _chunks;
    MultiTree _multitree;
	IdToChunkUMap _free_chunks;
	Time _min_time, _max_time;
	std::unique_ptr<SubscribeNotificator> _subscribe_notify;
    mutable std::mutex _subscribe_locker;
    mutable std::mutex _locker_free_chunks, _locker_drop, _locker_min_max;
    mutable std::mutex _locker_chunks;
    ChunkWriter*_cw;
	MemoryStorage::Limits _limits;
};

MemoryStorage::MemoryStorage(size_t size, MemoryStorage::Limits lmts)
	:_Impl(new MemoryStorage::Private(size,lmts)) {
}


MemoryStorage::~MemoryStorage() {
}

Time MemoryStorage::minTime() {
	return _Impl->minTime();
}

Time MemoryStorage::maxTime() {
	return _Impl->maxTime();
}

bool MemoryStorage::minMaxTime(dariadb::Id id, dariadb::Time*minResult, dariadb::Time*maxResult) {
	return _Impl->minMaxTime(id, minResult,maxResult);
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

void MemoryStorage::flush(){
	_Impl->flush();
}

size_t MemoryStorage::queue_size()const {
	return _Impl->queue_size();
}

dariadb::storage::ChunksList MemoryStorage::drop_old_chunks(const dariadb::Time min_time){
	return _Impl->drop_old_chunks(min_time);
}

dariadb::storage::ChunksList MemoryStorage::drop_old_chunks_by_limit(const size_t max_limit) {
	return _Impl->drop_old_chunks_by_limit(max_limit);
}
dariadb::storage::ChunksList MemoryStorage::drop_all()
{
	return _Impl->drop_all();
}

Cursor_ptr MemoryStorage::chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
	return _Impl->chunksByIterval(ids, flag, from, to);
}

IdToChunkMap MemoryStorage::chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
	return _Impl->chunksBeforeTimePoint(ids, flag, timePoint);
}

dariadb::IdArray MemoryStorage::getIds() {
	return _Impl->getIds();
}

bool MemoryStorage::append(const ChunksList&clist) {
    return _Impl->append(clist);
}

bool MemoryStorage::append(const Chunk_Ptr&c) {
    return _Impl->append(c);
}

void MemoryStorage::set_chunkWriter(ChunkWriter*cw){
    _Impl->set_chunkWriter(cw);
}
