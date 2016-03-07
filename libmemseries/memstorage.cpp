#include "memstorage.h"
#include "utils.h"
#include "compression.h"
#include <limits>
#include <algorithm>
#include <map>

using namespace memseries;
using namespace memseries::compression;
using namespace memseries::storage;
using memseries::utils::Range;



struct MeasChunk
{
    uint8_t *_buffer;
    Range times;
    Range flags;
    Range values;
    CopmressedWriter c_writer;
    size_t count;
    Meas first;

    Time minTime,maxTime;

    MeasChunk(size_t size, Meas first_m):
        count(0),
        first(first_m)
    {
        minTime=std::numeric_limits<Time>::max();
        maxTime=std::numeric_limits<Time>::min();

        _buffer=new uint8_t[size*3+3];

        times=Range{_buffer,_buffer+sizeof(uint8_t)*size};
        flags=Range{times.end+1,times.end+sizeof(uint8_t)*size};
        values=Range{flags.end+1,flags.end+sizeof(uint8_t)*size};

        using compression::BinaryBuffer;
        c_writer = compression::CopmressedWriter( BinaryBuffer(times.begin, times.end),
                                                  BinaryBuffer(values.begin, values.end),
                                                  BinaryBuffer(flags.begin, flags.end));
        c_writer.append(first);
        minTime=std::min(minTime,first_m.time);
        maxTime=std::max(maxTime,first_m.time);
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

            minTime=std::min(minTime,m.time);
            maxTime=std::max(maxTime,m.time);
            return true;
        }
    }


    bool is_full()const { return c_writer.is_full(); }
};

typedef std::shared_ptr<MeasChunk> Chunk_Ptr;
typedef std::vector<Chunk_Ptr> ChuncksVector;
typedef std::map<Id, ChuncksVector> ChunkMap;


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
        //TODO replace
        //return this->_chunks.size() == 0 && this->_tp_chunks.size() == 0;
    }


    void readNext(storage::ReaderClb*clb) override {
        if (!_tp_readed) {
            this->readTimePoint(clb);
        }


        for (auto ch : _chunks) {
            for (size_t i = 0; i < ch.second.size(); i++) {
                if (ch.second[i].chunk->maxTime>=_from){
                    auto cur_ch=ch.second[i].chunk;
                    CopmressedReader crr(BinaryBuffer(cur_ch->times.begin, cur_ch->times.end),
                                         BinaryBuffer(cur_ch->values.begin, cur_ch->values.end),
                                         BinaryBuffer(cur_ch->flags.begin, cur_ch->flags.end),
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

        }
        end=true;
        //TODO replace
        //_chunks.clear();
    }

    void readTimePoint(storage::ReaderClb*clb){

        std::list<InnerReader::ReadChunk> to_read_chunks{};
        for (auto ch : _tp_chunks) {
            auto candidate = ch.second.front();
            if (candidate.chunk->minTime > _from) {
                continue;
            }

            for (size_t i = 0; i < ch.second.size(); i++) {
                auto cur_chunk=ch.second[i].chunk;
                if ((candidate.chunk->first.time < cur_chunk->first.time) && (cur_chunk->first.time <= _from))
                {
                    candidate = ch.second[i];
                }
            }
            to_read_chunks.push_back(candidate);
        }

        for (auto ch : to_read_chunks) {
            CopmressedReader crr(BinaryBuffer(ch.chunk->times.begin, ch.chunk->times.end),
                                 BinaryBuffer(ch.chunk->values.begin, ch.chunk->values.end),
                                 BinaryBuffer(ch.chunk->flags.begin, ch.chunk->flags.end),
                                 ch.chunk->first);

            Meas candidate;
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
        //TODO replace
        //        _tp_chunks.clear();
    }


    bool is_time_point_reader;

    bool check_meas(const Meas&m)const{
        using utils::inInterval;

        if ((in_filter(_flag, m.flag))&&(inInterval(_from, _to, m.time))) {
            return true;
        }
        return false;
    }

    typedef std::vector<ReadChunk> ReadChuncksVector;
    typedef std::map<Id, ReadChuncksVector> ReadChunkMap;

    ReadChunkMap _chunks;
    ReadChunkMap _tp_chunks;
    memseries::Flag _flag;
    memseries::Time _from;
    memseries::Time _to;
    bool _tp_readed;
    //TODO remove end var
    bool end;
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
        }else {
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

    std::shared_ptr<InnerReader> readInterval(const IdArray &ids, Flag flag, Time from, Time to){
        auto res= this->readInTimePoint(ids,flag,from);
        res->_from=from;
        res->_to=to;
        res->_flag=flag;
        for (auto ch : _chuncks) {
            if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), ch.first) == ids.end())) {
                continue;
            }
            for (size_t i = 0; i < ch.second.size(); i++) {
                Chunk_Ptr cur_chunk = ch.second[i];
                if ((utils::inInterval(from,to,cur_chunk->minTime))||
                        (utils::inInterval(from,to,cur_chunk->maxTime))){
                    res->add(cur_chunk, cur_chunk->count);
                }
            }

        }
        res->is_time_point_reader=false;
        return res;
    }

    std::shared_ptr<InnerReader> readInTimePoint(const IdArray &ids, Flag flag, Time time_point){
        auto res = std::make_shared<InnerReader>(flag, time_point, 0);
        for (auto ch : _chuncks) {
            if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), ch.first) == ids.end())) {
                continue;
            }
            for (size_t i = 0; i < ch.second.size(); i++) {
                Chunk_Ptr cur_chunk = ch.second[i];
                if(cur_chunk->minTime<time_point){
                    res->add_tp(cur_chunk, cur_chunk->count);
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


MemoryStorage::MemoryStorage(size_t size){
    _Impl=new MemoryStorage::Private(size);
}


MemoryStorage::~MemoryStorage(){
    if(_Impl!=nullptr){
        delete _Impl;
    }
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


