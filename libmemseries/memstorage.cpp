#include "memstorage.h"
#include "utils.h"
#include "compression.h"
#include "flags.h"
#include <limits>
#include <algorithm>
#include <map>
#include <tuple>

using namespace memseries;
using namespace memseries::compression;
using namespace memseries::storage;
using memseries::utils::Range;



struct Chunk
{
    std::vector<uint8_t> _buffer_t;
	std::vector<uint8_t> _buffer_f;
	std::vector<uint8_t> _buffer_v;
    Range times;
    Range flags;
    Range values;
    CopmressedWriter c_writer;
    size_t count;
    Meas first;

    Time minTime,maxTime;

    Chunk(size_t size, Meas first_m):
        count(0),
        first(first_m)
    {
        minTime=std::numeric_limits<Time>::max();
        maxTime=std::numeric_limits<Time>::min();

		_buffer_t.resize(size);	std::fill(_buffer_t.begin(), _buffer_t.end(),0);
		_buffer_f.resize(size);	std::fill(_buffer_f.begin(), _buffer_f.end(), 0);
		_buffer_v.resize(size);	std::fill(_buffer_v.begin(), _buffer_v.end(), 0);

		times = Range{ _buffer_t.data(),_buffer_t.data() + sizeof(uint8_t)*size };
		flags = Range{ _buffer_f.data(),_buffer_f.data() + sizeof(uint8_t)*size };
		values = Range{ _buffer_v.data(),_buffer_v.data() + sizeof(uint8_t)*size };

        using compression::BinaryBuffer;
        c_writer = compression::CopmressedWriter( BinaryBuffer(times),
                                                  BinaryBuffer(values),
                                                  BinaryBuffer(flags));
        c_writer.append(first);
        minTime=std::min(minTime,first_m.time);
        maxTime=std::max(maxTime,first_m.time);
    }

    ~Chunk(){
       
    }
    bool append(const Meas&m)
    {
        auto t_f = this->c_writer.append(m);

        if (!t_f) {
            return false;
        }else{
            count++;

            minTime=std::min(minTime,m.time);
            maxTime=std::max(maxTime,m.time);
            return true;
        }
    }


    bool is_full()const { return c_writer.is_full(); }
};

typedef std::shared_ptr<Chunk>    Chunk_Ptr;
typedef std::list<Chunk_Ptr>      ChuncksList;
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
    InnerReader(memseries::Flag flag, memseries::Time from, memseries::Time to):
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
        ReadChunk rc;
        rc.chunk = c;
        rc.count = count;
        this->_chunks[c->first.id].push_back(rc);
    }

    void add_tp(Chunk_Ptr c, size_t count){
        ReadChunk rc;
        rc.chunk = c;
        rc.count = count;
        this->_tp_chunks[c->first.id].push_back(rc);
    }

    bool isEnd() const override{
        return this->end && this->_tp_readed;
    }

	memseries::IdArray getIds()const override {
		memseries::IdArray result;
		result.resize(_chunks.size());
		size_t pos = 0;
		for (auto &kv : _chunks) {
			result[pos] = kv.first;
			pos++;
		}
		return result;
	}

    void readNext(storage::ReaderClb*clb) override {
        if (!_tp_readed) {
            this->readTimePoint(clb);
        }

        for (auto ch : _chunks) {
            for (size_t i = 0; i < ch.second.size(); i++) {
                auto cur_ch=ch.second[i].chunk;
                CopmressedReader crr(BinaryBuffer(cur_ch->times),
                                     BinaryBuffer(cur_ch->values),
                                     BinaryBuffer(cur_ch->flags),
                                     cur_ch->first);

                if (check_meas(ch.second[i].chunk->first)) {
                    auto sub=ch.second[i].chunk->first;
                    clb->call(sub);
                }

                for (size_t j = 0; j < ch.second[i].count; j++) {
                    auto sub = crr.read();
                    sub.id = ch.second[i].chunk->first.id;
                    if (check_meas(sub)) {
                        clb->call(sub);
                    }
                }

            }

        }
        end=true;
    }

    void readTimePoint(storage::ReaderClb*clb){

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
            CopmressedReader crr(BinaryBuffer(ch.chunk->times),
                                 BinaryBuffer(ch.chunk->values),
                                 BinaryBuffer(ch.chunk->flags),
                                 ch.chunk->first);

            Meas candidate;
            candidate = ch.chunk->first;
            for (size_t i = 0; i < ch.count; i++) {
                auto sub = crr.read();
                sub.id = ch.chunk->first.id;
                if ((sub.time <= _from) && (sub.time >= candidate.time)) {
                    candidate = sub;
                }
            }
            if (candidate.time <= _from) {
				//TODO make as options
				candidate.time = _from;

                clb->call(candidate);
				_tp_readed_times.insert(std::make_tuple(candidate.id, candidate.time));
            }
        }
        auto m=memseries::Meas::empty();
        m.time=_from;
        m.flag=memseries::Flags::NO_DATA;
        for(auto id:_not_exist){
            m.id=id;
            clb->call(m);
        }
        _tp_readed = true;
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
    memseries::Flag _flag;
    memseries::Time _from;
    memseries::Time _to;
    bool _tp_readed;
    bool end;
    IdArray _not_exist;

	typedef std::tuple<memseries::Id, memseries::Time> IdTime;
	std::set<IdTime> _tp_readed_times;
};


class MemoryStorage::Private
{
public:
    Private(size_t size):
        _size(size),
        _min_time(std::numeric_limits<memseries::Time>::max()),
        _max_time(std::numeric_limits<memseries::Time>::min())
    {}

    ~Private(){
        _chuncks.clear();
    }

    Time minTime(){return _min_time;}
    Time maxTime(){return _max_time;}

    Chunk_Ptr getFreeChunk(memseries::Id id){
        Chunk_Ptr resulted_chunk=nullptr;
        auto ch_iter=_chuncks.find(id);
        if (ch_iter != _chuncks.end()) {
            for (auto &v:ch_iter->second) {
                if (!v->is_full()) {
                    resulted_chunk = v;
                    break;
                }
            }
        }else {
            this->_chuncks[id] = ChuncksList{};
        }
        return resulted_chunk;
    }

    append_result append(const Meas& value){
        Chunk_Ptr chunk=this->getFreeChunk(value.id);

        bool need_sort = false;

        if (chunk == nullptr) {
            chunk = std::make_shared<Chunk>(_size, value);
            this->_chuncks[value.id].push_back(chunk);
            need_sort = true;
        }
        else {
            if (!chunk->append(value)) {
                chunk = std::make_shared<Chunk>(_size, value);
                this->_chuncks[value.id].push_back(chunk);
                need_sort = true;
            }
        }

        if (need_sort){
			this->_chuncks[value.id].sort(
				[](const Chunk_Ptr &l, const Chunk_Ptr &r) {
				return l->first.time < r->first.time; 
			});
        }

        _min_time = std::min(_min_time, value.time);
        _max_time = std::max(_max_time, value.time);
        return memseries::append_result(1, 0);
    }


    append_result append(const Meas::PMeas begin, const size_t size){
        memseries::append_result result{};
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
        
        for (auto ch : _chuncks) {
            if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), ch.first) == ids.end())) {
                continue;
            }
			for (auto &cur_chunk : ch.second) {
				if ((utils::inInterval(from, to, cur_chunk->minTime)) ||
					(utils::inInterval(from, to, cur_chunk->maxTime))) {
					res->add(cur_chunk, cur_chunk->count);
				}
			}

        }
        res->is_time_point_reader=false;
        return res;
    }

    std::shared_ptr<InnerReader> readInTimePoint(const IdArray &ids, Flag flag, Time time_point){
		auto res = std::make_shared<InnerReader>(flag, time_point, 0);
		res->is_time_point_reader = true;
		
        if(ids.size()==0){
            for (auto ch : _chuncks) {
				load_tp_from_chunks(res.get(), ch.second, time_point, ch.first);
            }
        }else{
            for(auto id:ids){
                auto search_res=_chuncks.find(id);
                if(search_res==_chuncks.end()){
                    res->_not_exist.push_back(id);
                }else{
                    auto ch=search_res->second;
					load_tp_from_chunks(res.get(), ch, time_point, id);
                }
            }
        }
        return res;
    }

	void load_tp_from_chunks(InnerReader *_ptr, ChuncksList chunks, Time time_point, Id id) {
		bool is_exists = false;
		for (auto&cur_chunk : chunks) {
			if (cur_chunk->minTime <= time_point) {
				_ptr->add_tp(cur_chunk, cur_chunk->count);
				is_exists = true;
			}
		}
		if (!is_exists) {
			_ptr->_not_exist.push_back(id);
		}
	}

    size_t size()const { return _size; }
    size_t chunks_size()const { return _chuncks.size(); }

	void subscribe(const IdArray&ids, Flag flag, ReaderClb_ptr clbk) {
		NOT_IMPLEMENTED;
	}
protected:
    size_t _size;

    ChunkMap _chuncks;
    Time _min_time,_max_time;
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

append_result MemoryStorage::append(const memseries::Meas &value){
    return _Impl->append(value);
}

append_result MemoryStorage::append(const memseries::Meas::PMeas begin, const size_t size){
    return _Impl->append(begin,size);
}

Reader_ptr MemoryStorage::readInterval(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time from, memseries::Time to){
    return _Impl->readInterval(ids,flag,from,to);
}

Reader_ptr  MemoryStorage::readInTimePoint(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time time_point){
    return _Impl->readInTimePoint(ids,flag,time_point);
}

size_t  MemoryStorage::size()const {
    return _Impl->size();
}

size_t  MemoryStorage::chunks_size()const {
    return _Impl->chunks_size();
}


void MemoryStorage::subscribe(const IdArray&ids, Flag flag, ReaderClb_ptr clbk) {
	return _Impl->subscribe(ids, flag, clbk);
}