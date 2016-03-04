#include "memstorage.h"
#include "utils.h"
#include <limits>
#include <algorithm>

memseries::storage::MemoryStorage::Block::Block(size_t size)
{
    begin = new uint8_t[size];
    end = begin + size;

    memset(begin, 0, size);
}

memseries::storage::MemoryStorage::Block::~Block()
{
    delete[] begin;
}

memseries::storage::MemoryStorage::MeasChunk::MeasChunk(size_t size, Meas first_m) :
    times(size),
    flags(size),
    values(size),
    count(0),
    first(first_m),
    last(first_m)
{
    c_writer = memseries::compression::CopmressedWriter(
        memseries::compression::BinaryBuffer(times.begin, times.end),
        memseries::compression::BinaryBuffer(values.begin, values.end),
        memseries::compression::BinaryBuffer(flags.begin, flags.end));
    c_writer.append(first);
}

memseries::storage::MemoryStorage::MeasChunk::~MeasChunk()
{

}

bool memseries::storage::MemoryStorage::MeasChunk::append(const Meas & m)
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

memseries::storage::MemoryStorage::MemoryStorage(size_t size):
    _size(size),
    _min_time(std::numeric_limits<memseries::Time>::max()),
    _max_time(std::numeric_limits<memseries::Time>::min())
{
}


memseries::storage::MemoryStorage::~MemoryStorage()
{
    _chuncks.clear();
}

memseries::Time memseries::storage::MemoryStorage::minTime()
{
    return _min_time;
}

memseries::Time memseries::storage::MemoryStorage::maxTime()
{
    return _max_time;
}

memseries::append_result memseries::storage::MemoryStorage::append(const memseries::Meas &value)
{
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
        this->_chuncks[value.id] = memseries::storage::MemoryStorage::ChuncksVector{};
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
        std::sort(
            this->_chuncks[value.id].begin(),
            this->_chuncks[value.id].end(),
            [](Chunk_Ptr &l, Chunk_Ptr &r) {return l->first.time < r->first.time; });
    }

    _min_time = std::min(_min_time, value.time);
    _max_time = std::max(_max_time, value.time);
    return memseries::append_result(1, 0);
}

memseries::append_result memseries::storage::MemoryStorage::append(const memseries::Meas::PMeas begin, const size_t size)
{
    memseries::append_result result{};
    for (size_t i = 0; i < size; i++) {
        result = result + append(begin[i]);
    }
    return result;
}

memseries::storage::Reader_ptr memseries::storage::MemoryStorage::readInterval(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time from, memseries::Time to)
{
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

memseries::storage::Reader_ptr memseries::storage::MemoryStorage::readInTimePoint(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time time_point)
{
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


memseries::storage::MemoryStorage::InnerReader::InnerReader(memseries::Flag flag, memseries::Time from, memseries::Time to):
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

void memseries::storage::MemoryStorage::InnerReader::add(Chunk_Ptr c, size_t count)
{
    memseries::storage::MemoryStorage::InnerReader::ReadChunk rc;
    rc.chunk = c;
    rc.count = count;
    this->_chunks[c->first.id].push_back(rc);
}

bool memseries::storage::MemoryStorage::InnerReader::isEnd() const
{
    return this->_chunks.size() == 0 && _next.count == 0 && _cur_vector.size() == 0 && _cur_vector_pos >= _cur_vector.size();
}

bool  memseries::storage::MemoryStorage::InnerReader::check_meas(memseries::Meas&m) {
    if ((memseries::in_filter(_flag, m.flag))	&& (memseries::utils::inInterval(_from, _to, m.time))) {
        return true;
    }
    return false;
}

void memseries::storage::MemoryStorage::InnerReader::readNext(memseries::Meas::MeasList *output)
{
    if (!_tp_readed) {
        this->readTimePoint(output);
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
                output->push_back(_next.chunk->first);
            }
        }
    }

    while (_next.count != 0) {
        memseries::compression::CopmressedReader crr(
            memseries::compression::BinaryBuffer(_next.chunk->times.begin, _next.chunk->times.end),
            memseries::compression::BinaryBuffer(_next.chunk->values.begin, _next.chunk->values.end),
            memseries::compression::BinaryBuffer(_next.chunk->flags.begin, _next.chunk->flags.end), _next.chunk->first);

        for (size_t i = 0; i < _next.count; i++) {
            auto sub = crr.read();
            if (check_meas(sub)) {
                sub.id = _next.chunk->first.id;
                output->push_back(sub);
            }
        }
        _next.count = 0;
    }
}

void memseries::storage::MemoryStorage::InnerReader::readTimePoint(Meas::MeasList * output)
{
    std::list<memseries::storage::MemoryStorage::InnerReader::ReadChunk> to_read_chunks{};
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
        memseries::compression::CopmressedReader crr(
            memseries::compression::BinaryBuffer(ch.chunk->times.begin, ch.chunk->times.end),
            memseries::compression::BinaryBuffer(ch.chunk->values.begin, ch.chunk->values.end),
            memseries::compression::BinaryBuffer(ch.chunk->flags.begin, ch.chunk->flags.end), ch.chunk->first);

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
            output->push_back(candidate);
        }
    }
    _tp_readed = true;
}
