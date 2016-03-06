#include "memstorage.h"
#include "utils.h"
#include "compression.h"
#include <limits>
#include <algorithm>
#include <map>

using namespace memseries;

struct Block
{
    uint8_t *begin;
    uint8_t *end;
    Block(){
        begin=end=nullptr;
    }

    Block(uint8_t *_begin, uint8_t *_end){
        begin=_begin;
        end=_end;
    }
};


struct MeasChunk
{
    uint8_t *_buffer;
    Block times;
    Block flags;
    Block values;
    compression::CopmressedWriter c_writer;
    size_t count;
    Meas first,last;

    MeasChunk(size_t size, Meas first_m):
        count(0),
        first(first_m),
        last(first_m)
    {
        _buffer=new uint8_t[size*3+3];

        times=Block{_buffer,_buffer+sizeof(uint8_t)*size};
        flags=Block{times.end+1,times.end+sizeof(uint8_t)*size};
        values=Block{flags.end+1,flags.end+sizeof(uint8_t)*size};

        using compression::BinaryBuffer;
        c_writer = compression::CopmressedWriter( BinaryBuffer(times.begin, times.end),
                                                  BinaryBuffer(values.begin, values.end),
                                                  BinaryBuffer(flags.begin, flags.end));
        c_writer.append(first);
    }

    ~MeasChunk(){
        delete[] _buffer;
    }
    bool append(const Meas&m)
    {
        auto t_f = this->c_writer.append(m);

        if (!t_f) {
            return false;
        }else{
            count++;
            if(m.time>=last.time){
                last=m;
            }
            return true;
        }
    }


    bool is_full()const { return c_writer.is_full(); }
};

typedef std::shared_ptr<MeasChunk> Chunk_Ptr;
typedef std::vector<Chunk_Ptr> ChuncksVector;
typedef std::map<Id, ChuncksVector> ChunkMap;


class InnerReader: public storage::Reader{
public:
    struct ReadChunk
    {
        size_t    count;
        Chunk_Ptr chunk;
    };
    InnerReader(memseries::Flag flag, memseries::Time from, memseries::Time to):
        _chunks{},
        _flag(flag),
        _from(from),
        _to(to),
        _cur_vector_pos(0),
        _tp_readed(false)
    {
        _next.chunk = nullptr;
        _next.count = 0;
        is_time_point_reader = false;
    }

    void add(Chunk_Ptr c, size_t count){
        ReadChunk rc;
        rc.chunk = c;
        rc.count = count;
        this->_chunks[c->first.id].push_back(rc);
    }


    bool isEnd() const override{
        return this->_chunks.size() == 0
                && _next.count == 0
                && _cur_vector.size() == 0
                && _cur_vector_pos >= _cur_vector.size();
    }


    void readNext(storage::ReaderClb*clb) override {
        if (!_tp_readed) {
            this->readTimePoint(clb);
        }

        if (is_time_point_reader) {
            _chunks.clear();
            return;
        }

        if ((_next.chunk == nullptr) || (_next.count == 0)) {
            if ((_cur_vector_pos == _cur_vector.size()) || (_cur_vector.size()==0)) {
                _cur_vector.clear();
                _cur_vector_pos = 0;
                if (_chunks.size() != 0) {
                    auto cur_pos = _chunks.begin();
                    _cur_vector = cur_pos->second;
                    _chunks.erase(cur_pos);
                }
                else {
                    return;
                }
            }
            if (_cur_vector.size() != 0) {
                auto fr = _cur_vector[_cur_vector_pos];
                _next.chunk = fr.chunk;
                _next.count = fr.count;
                _cur_vector_pos++;

                if (check_meas(_next.chunk->first)) {
                    clb->call(_next.chunk->first);
                }
            }
        }
        using compression::BinaryBuffer;
        while (_next.count != 0) {
            compression::CopmressedReader crr(
                        BinaryBuffer(_next.chunk->times.begin, _next.chunk->times.end),
                        BinaryBuffer(_next.chunk->values.begin, _next.chunk->values.end),
                        BinaryBuffer(_next.chunk->flags.begin, _next.chunk->flags.end),
                        _next.chunk->first);

            for (size_t i = 0; i < _next.count; i++) {
                auto sub = crr.read();
                if (check_meas(sub)) {
                    sub.id = _next.chunk->first.id;
                    clb->call(sub);
                }
            }
            _next.count = 0;
        }
    }

    void readTimePoint(storage::ReaderClb*clb){
        std::list<InnerReader::ReadChunk> to_read_chunks{};
        for (auto ch : _chunks) {
            auto candidate = ch.second.front();
            if (candidate.chunk->first.time > _from) {
                continue;
            }

            for (size_t i = 1; i < ch.second.size(); i++) {
                if ((candidate.chunk->first.time < ch.second[i].chunk->first.time)
                        && (ch.second[i].chunk->first.time <= _from)) {
                    candidate = ch.second[i];
                }
            }
            to_read_chunks.push_back(candidate);
        }

        for (auto ch : to_read_chunks) {
            compression::CopmressedReader crr(compression::BinaryBuffer(ch.chunk->times.begin, ch.chunk->times.end),
                                              compression::BinaryBuffer(ch.chunk->values.begin, ch.chunk->values.end),
                                              compression::BinaryBuffer(ch.chunk->flags.begin, ch.chunk->flags.end), ch.chunk->first);

            memseries::Meas candidate;
            candidate = ch.chunk->first;
            for (size_t i = 1; i < ch.count; i++) {
                auto sub = crr.read();
                sub.id = ch.chunk->first.id;
                if ((sub.time <= _from) && (sub.time >= candidate.time)) {
                    candidate = sub;
                }
            }
            if (candidate.time <= _from) {
                clb->call(candidate);
            }
        }
        _tp_readed = true;
    }


    bool is_time_point_reader;
protected:
    bool check_meas(Meas&m){
        if ((memseries::in_filter(_flag, m.flag))	&& (memseries::utils::inInterval(_from, _to, m.time))) {
            return true;
        }
        return false;
    }

    typedef std::vector<ReadChunk> ReadChuncksVector;
    typedef std::map<Id, ReadChuncksVector> ReadChunkMap;
protected:
    ReadChunkMap _chunks;
    memseries::Flag _flag;
    memseries::Time _from;
    memseries::Time _to;
    ReadChunk _next;
    ReadChuncksVector _cur_vector;
    size_t _cur_vector_pos;
    bool _tp_readed;
};


class storage::MemoryStorage::Private
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

    append_result append(const Meas& value){

        Chunk_Ptr chunk=nullptr;
        auto ch_iter=_chuncks.find(value.id);
        if (ch_iter != _chuncks.end()) {
            for (size_t i = 0; i < ch_iter->second.size(); i++) {
                if (!ch_iter->second[i]->is_full()) {
                    chunk = ch_iter->second[i];
                    break;
                }
            }
        }

        else {
            this->_chuncks[value.id] = ChuncksVector{};
        }

        bool need_sort = false;

        if (chunk == nullptr) {
            chunk = std::make_shared<MeasChunk>(_size, value);
            this->_chuncks[value.id].push_back(chunk);
            need_sort = true;
        }
        else {
            if (!chunk->append(value)) {
                chunk = std::make_shared<MeasChunk>(_size, value);
                this->_chuncks[value.id].push_back(chunk);
                need_sort = true;
            }
        }

        if (need_sort){
            std::sort(this->_chuncks[value.id].begin(),
                    this->_chuncks[value.id].end(),
                    [](const Chunk_Ptr &l, const Chunk_Ptr &r) {return l->first.time < r->first.time; });
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

    Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to){
        auto res= std::make_shared<InnerReader>(flag, from, to);
        for (auto ch : _chuncks) {
            if ((ids.size() == 0) || (std::find(ids.begin(), ids.end(), ch.first) != ids.end())) {
                for (size_t i = 0; i < ch.second.size(); i++) {
                    auto cur_chunk = ch.second[i];
                    res->add(cur_chunk, cur_chunk->count);
                }
            }
        }

        return res;
    }

    Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point){
        auto res = std::make_shared<InnerReader>(flag, time_point, 0);
        for (auto ch : _chuncks) {
            if ((ids.size() == 0) || (std::find(ids.begin(), ids.end(), ch.first) != ids.end())) {
                for (size_t i = 0; i < ch.second.size(); i++) {
                    auto cur_chunk = ch.second[i];
                    res->add(cur_chunk, cur_chunk->count);
                }
            }
        }
        res->is_time_point_reader = true;
        return res;
    }

    size_t size()const { return _size; }
    size_t chunks_size()const { return _chuncks.size(); }
protected:
    size_t _size;

    ChunkMap _chuncks;
    Time _min_time,_max_time;
};


memseries::storage::MemoryStorage::MemoryStorage(size_t size){
    _Impl=new MemoryStorage::Private(size);
}


memseries::storage::MemoryStorage::~MemoryStorage(){
    if(_Impl!=nullptr){
        delete _Impl;
    }
}

memseries::Time memseries::storage::MemoryStorage::minTime(){
    return _Impl->minTime();
}

memseries::Time memseries::storage::MemoryStorage::maxTime(){
    return _Impl->maxTime();
}

memseries::append_result memseries::storage::MemoryStorage::append(const memseries::Meas &value){
    return _Impl->append(value);
}

memseries::append_result memseries::storage::MemoryStorage::append(const memseries::Meas::PMeas begin, const size_t size){
    return _Impl->append(begin,size);
}

memseries::storage::Reader_ptr memseries::storage::MemoryStorage::readInterval(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time from, memseries::Time to){
    return _Impl->readInterval(ids,flag,from,to);
}

memseries::storage::Reader_ptr memseries::storage::MemoryStorage::readInTimePoint(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time time_point){
    return _Impl->readInTimePoint(ids,flag,time_point);
}

size_t memseries::storage::MemoryStorage::size()const {
    return _Impl->size();
}

size_t memseries::storage::MemoryStorage::chunks_size()const {
    return _Impl->chunks_size();
}


