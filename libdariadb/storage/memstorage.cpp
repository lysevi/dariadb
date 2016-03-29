#include "memstorage.h"
#include "../utils/utils.h"
#include "../compression.h"
#include "../flags.h"
#include "subscribe.h"
#include "chunk.h"
#include "../timeutil.h"

#include <limits>
#include <algorithm>
#include <map>
#include <tuple>
#include <assert.h>
#include <mutex>

using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

typedef std::map<Id, ChuncksList> ChunkMap;

class InnerReader: public Reader{
public:
    struct ReadChunk
    {
        size_t    count;
        Chunk_Ptr chunk;
        ReadChunk()=default;
        ReadChunk(const ReadChunk&other){
            count=other.count;
            chunk=other.chunk;
        }
        ReadChunk&operator=(const ReadChunk&other){
            if(this!=&other){
                count=other.count;
                chunk=other.chunk;
            }
            return *this;
        }
    };
    InnerReader(dariadb::Flag flag, dariadb::Time from, dariadb::Time to):
        _chunks{},
        _flag(flag),
        _from(from),
        _to(to),
        _tp_readed(false)
    {
        is_time_point_reader = false;
        end=false;
    }

    void add(Chunk_Ptr c, size_t count){
		std::lock_guard<std::mutex> lg(_mutex);
        ReadChunk rc;
        rc.chunk = c;
        rc.count = count;
        this->_chunks[c->first.id].push_back(rc);
    }

    void add_tp(Chunk_Ptr c, size_t count){
		std::lock_guard<std::mutex> lg(_mutex);
        ReadChunk rc;
        rc.chunk = c;
        rc.count = count;
        this->_tp_chunks[c->first.id].push_back(rc);
    }

    bool isEnd() const override{
        return this->end && this->_tp_readed;
    }

	dariadb::IdArray getIds()const override {
		dariadb::IdArray result;
		result.resize(_chunks.size());
		size_t pos = 0;
		for (auto &kv : _chunks) {
			result[pos] = kv.first;
			pos++;
		}
		return result;
	}

    void readNext(storage::ReaderClb*clb) override {
		std::lock_guard<std::mutex> lg(_mutex);
		
        if (!_tp_readed) {
            this->readTimePoint(clb);
        }

        for (auto ch : _chunks) {
            for (size_t i = 0; i < ch.second.size(); i++) {
                auto cur_ch=ch.second[i].chunk;
                auto bw=std::make_shared<BinaryBuffer>(cur_ch->bw->get_range());
                bw->reset_pos();
                CopmressedReader crr(bw, cur_ch->first);

                if (check_meas(ch.second[i].chunk->first)) {
                    auto sub=ch.second[i].chunk->first;
                    clb->call(sub);
                }

                for (size_t j = 0; j < ch.second[i].count; j++) {
                    auto sub = crr.read();
                    sub.id = ch.second[i].chunk->first.id;
                    if (check_meas(sub)) {
                        clb->call(sub);
                    }else{
                        if(sub.time>_to){
                            break;
                        }
                    }
                }

            }
        }
        end=true;
    }

    void readTimePoint(storage::ReaderClb*clb){
        std::lock_guard<std::mutex> lg(_mutex_tp);
        std::list<InnerReader::ReadChunk> to_read_chunks{};
        for (auto ch : _tp_chunks) {
            auto candidate = ch.second.front();

            for (size_t i = 0; i < ch.second.size(); i++) {
                auto cur_chunk=ch.second[i].chunk;
                if (candidate.chunk->first.time < cur_chunk->first.time){
                    candidate = ch.second[i];
                }
            }
            to_read_chunks.push_back(candidate);
        }

        for (auto ch : to_read_chunks) {
            auto bw=std::make_shared<BinaryBuffer>(ch.chunk->bw->get_range());
            bw->reset_pos();
            CopmressedReader crr(bw, ch.chunk->first);

            Meas candidate;
            candidate = ch.chunk->first;
            for (size_t i = 0; i < ch.count; i++) {
                auto sub = crr.read();
                sub.id = ch.chunk->first.id;
                if ((sub.time <= _from) && (sub.time >= candidate.time)) {
                    candidate = sub;
                }if(sub.time>_from){
                    break;
                }
            }
            if (candidate.time <= _from) {
				//TODO make as options
				candidate.time = _from;

                clb->call(candidate);
				_tp_readed_times.insert(std::make_tuple(candidate.id, candidate.time));
            }
        }
        auto m=dariadb::Meas::empty();
        m.time=_from;
        m.flag=dariadb::Flags::NO_DATA;
        for(auto id:_not_exist){
            m.id=id;
            clb->call(m);
        }
        _tp_readed=true;
    }


    bool is_time_point_reader;

    bool check_meas(const Meas&m)const{
		auto tmp = std::make_tuple(m.id, m.time);
		if (this->_tp_readed_times.find(tmp) != _tp_readed_times.end()) {
			return false;
		}
        using utils::inInterval;

        if ((in_filter(_flag, m.flag))&&(inInterval(_from, _to, m.time))) {
            return true;
        }
        return false;
    }

	Reader_ptr clone()const override{
		auto res= std::make_shared<InnerReader>(_flag, _from,_to);
		res->_chunks = _chunks;
		res->_tp_chunks = _tp_chunks;
		res->_flag = _flag;
		res->_from = _from;
		res->_to = _to;
		res->_tp_readed = _tp_readed;
		res->end = end;
		res->_not_exist = _not_exist;
		res->_tp_readed_times = _tp_readed_times;
		return res;
	}
	void reset()override {
		end = false;
		_tp_readed = false;
		_tp_readed_times.clear();
	}
    typedef std::vector<ReadChunk> ReadChuncksVector;
    typedef std::map<Id, ReadChuncksVector> ReadChunkMap;

    ReadChunkMap _chunks;
    ReadChunkMap _tp_chunks;
    dariadb::Flag _flag;
    dariadb::Time _from;
    dariadb::Time _to;
    bool _tp_readed;
    bool end;
    IdArray _not_exist;

	typedef std::tuple<dariadb::Id, dariadb::Time> IdTime;
	std::set<IdTime> _tp_readed_times;

    std::mutex _mutex,_mutex_tp;

	
};

class InnerCurrentValuesReader : public Reader {
public:
	InnerCurrentValuesReader() {
		this->end = false;
	}
	~InnerCurrentValuesReader() {}

	bool isEnd() const override {
		return this->end;
	}

	void readCurVals(storage::ReaderClb*clb) {
		for (auto v : _cur_values) {
			clb->call(v);
		}
	}

	void readNext(storage::ReaderClb*clb) override {
		std::lock_guard<std::mutex> lg(_mutex);
		readCurVals(clb);
		this->end = true;
	}

	IdArray getIds()const {
		dariadb::IdArray result;
		result.resize(_cur_values.size());
		size_t pos = 0;
		for (auto v : _cur_values) {
			result[pos] = v.id;
			pos++;
		}
		return result;
	}
	Reader_ptr clone()const {
		auto raw_reader = new InnerCurrentValuesReader();
		Reader_ptr result{ raw_reader };
		raw_reader->_cur_values = _cur_values;
		return result;
	}
	void reset() {
		end = false;
	}
	bool end;
	std::mutex _mutex;
	dariadb::Meas::MeasList _cur_values;
};

class MemoryStorage::Private
{
public:
    Private(size_t size):
        _size(size),
        _min_time(std::numeric_limits<dariadb::Time>::max()),
        _max_time(std::numeric_limits<dariadb::Time>::min()),
		_subscribe_notify(new SubscribeNotificator)
    {
		_subscribe_notify->start();
	}

    ~Private(){
		_subscribe_notify->stop();
        _chuncks.clear();
    }

    Time minTime(){return _min_time;}
    Time maxTime(){return _max_time;}

    Chunk_Ptr getFreeChunk(dariadb::Id id){
        Chunk_Ptr resulted_chunk=nullptr;
		auto ch_iter = _free_chunks.find(id);
		if (ch_iter != _free_chunks.end()) {
			if (!ch_iter->second->is_full()){
				return ch_iter->second;
			}
        }
        return resulted_chunk;
    }

    append_result append(const Meas& value){
		std::lock_guard<std::mutex> lg(_mutex);

        Chunk_Ptr chunk=this->getFreeChunk(value.id);

       
        if (chunk == nullptr) {
            chunk = std::make_shared<Chunk>(_size, value);
            this->_chuncks[value.id].push_back(chunk);
			this->_free_chunks[value.id] = chunk;
       
        }
        else {
            if (!chunk->append(value)) {
                chunk = std::make_shared<Chunk>(_size, value);
                this->_chuncks[value.id].push_back(chunk);
       
            }
        }

        _min_time = std::min(_min_time, value.time);
        _max_time = std::max(_max_time, value.time);

		_subscribe_notify->on_append(value);

        return dariadb::append_result(1, 0);
    }


    append_result append(const Meas::PMeas begin, const size_t size){
        dariadb::append_result result{};
        for (size_t i = 0; i < size; i++) {
            result = result + append(begin[i]);
        }
        return result;

    }

    std::shared_ptr<InnerReader> readInterval(const IdArray &ids, Flag flag, Time from, Time to){
		
		std::shared_ptr<InnerReader> res;
		if (from > this->minTime())  {
			res = this->readInTimePoint(ids, flag, from);
			res->_from = from;
			res->_to = to;
			res->_flag = flag;
		}
		else {
			res= std::make_shared<InnerReader>(flag, from, to);
		}
        
		auto neededChunks = chunksByIterval(ids, flag, from, to);
        for (auto cur_chunk : neededChunks) {
			res->add(cur_chunk, cur_chunk->count);
        }
        res->is_time_point_reader=false;
        return res;
    }

	std::shared_ptr<InnerReader> readInTimePoint(const IdArray &ids, Flag flag, Time time_point) {

		auto res = std::make_shared<InnerReader>(flag, time_point, 0);
		res->is_time_point_reader = true;

		auto chunks_before = chunksBeforeTimePoint(ids, flag, time_point);
		IdArray target_ids{ids};
		if (target_ids.size() == 0) {
			target_ids = getIds();
		}

		for (auto id : target_ids) {
			auto search_res = chunks_before.find(id);
			if (search_res == chunks_before.end()) {
				res->_not_exist.push_back(id);
			}
			else {
				auto ch = search_res->second;
				res->add_tp(ch, ch->count);
			}
		}

        return res;
    }

    size_t size()const { return _size; }
    size_t chunks_size()const { return _chuncks.size(); }

	size_t chunks_total_size()const { 
		size_t result = 0;
		for (auto kv : _chuncks) {
			result += kv.second.size();
		}
		return result;
	}

    void subscribe(const IdArray&ids,const Flag& flag, const ReaderClb_ptr &clbk) {
        auto new_s=std::make_shared<SubscribeInfo>(ids,flag,clbk);
		_subscribe_notify->add(new_s);
	}

	Reader_ptr currentValue(const IdArray&ids, const Flag& flag) {
		std::lock_guard<std::mutex> lg(_mutex_tp);
		auto res_raw = new InnerCurrentValuesReader();
		Reader_ptr res{ res_raw };
		for (auto &kv: _free_chunks) {
			auto l = kv.second->last;
			if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), l.id) == ids.end())) {
				continue;
			}
			if ((flag==0) || (l.flag == flag)) {
				res_raw->_cur_values.push_back(l);
			}
		}
		return res;
	}

	dariadb::storage::ChuncksList drop_old_chunks(const dariadb::Time min_time) {
		std::lock_guard<std::mutex> lg(_mutex_tp);
		ChuncksList result;
        auto now=dariadb::timeutil::current_time();

		for (auto& kv : _chuncks) {
			while((kv.second.size()>0)) {
                auto past=(now-min_time);
                if(kv.second.front()->maxTime< past) {
					result.push_back(kv.second.front());
					kv.second.pop_front();
				}
				else {
					break;
				}
			}
		}
		return result;
	}

	ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
		ChuncksList result{};

		for (auto ch : _chuncks) {
			if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), ch.first) == ids.end())) {
				continue;
			}
			for (auto &cur_chunk : ch.second) {
				if (flag != 0) {
					if (!cur_chunk->check_flag(flag)) {
						continue;
					}
				}
				if ((utils::inInterval(from, to, cur_chunk->minTime)) || (utils::inInterval(from, to, cur_chunk->maxTime))) {
					result.push_back(cur_chunk);
				}
			}

		}
		return result;
	}

	IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
		IdToChunkMap result;
		for (auto ch : _chuncks) {
			if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), ch.first) == ids.end())) {
				continue;
			}
			auto chunks = &ch.second;
			for (auto it = chunks->rbegin(); it != chunks->crend(); ++it) {
				auto cur_chunk = *it;
				if (!cur_chunk->check_flag(flag)) {
					continue;
				}
				if (cur_chunk->minTime <= timePoint) {
					result[ch.first] = cur_chunk;
					break;
				}
			}
		}
		return result;
	}

	dariadb::IdArray getIds()const {
		dariadb::IdArray result;
		result.resize(_chuncks.size());
		size_t pos = 0;
		for (auto&kv : _chuncks) {
			result[pos] = kv.first;
			pos++;
		}
		return result;
	}
protected:
    size_t _size;

    ChunkMap _chuncks;
	IdToChunkMap _free_chunks;
    Time _min_time,_max_time;
	std::unique_ptr<SubscribeNotificator> _subscribe_notify;
	std::mutex _mutex;
    std::mutex _mutex_tp;
};


MemoryStorage::MemoryStorage(size_t size)
    :_Impl(new MemoryStorage::Private(size)){
}


MemoryStorage::~MemoryStorage(){
}

Time MemoryStorage::minTime(){
    return _Impl->minTime();
}

Time MemoryStorage::maxTime(){
    return _Impl->maxTime();
}

append_result MemoryStorage::append(const dariadb::Meas &value){
    return _Impl->append(value);
}

append_result MemoryStorage::append(const dariadb::Meas::PMeas begin, const size_t size){
    return _Impl->append(begin,size);
}

Reader_ptr MemoryStorage::readInterval(const dariadb::IdArray &ids, dariadb::Flag flag, dariadb::Time from, dariadb::Time to){
    return _Impl->readInterval(ids,flag,from,to);
}

Reader_ptr  MemoryStorage::readInTimePoint(const dariadb::IdArray &ids, dariadb::Flag flag, dariadb::Time time_point){
    return _Impl->readInTimePoint(ids,flag,time_point);
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

void MemoryStorage::subscribe(const IdArray&ids,const Flag& flag, const ReaderClb_ptr &clbk) {
	return _Impl->subscribe(ids, flag, clbk);
}

Reader_ptr MemoryStorage::currentValue(const IdArray&ids, const Flag& flag) {
	return  _Impl->currentValue(ids, flag);
}

void MemoryStorage::flush()
{
}

dariadb::storage::ChuncksList MemoryStorage::drop_old_chunks(const dariadb::Time min_time)
{
	return _Impl->drop_old_chunks(min_time);
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