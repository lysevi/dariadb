#include "inner_readers.h"
#include "../flags.h"
#include <cassert>

using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

class CursorReader :public dariadb::storage::Cursor::Callback {
public:
	Chunk_Ptr readed;
	CursorReader() {
		readed = nullptr;
	}
	void call(dariadb::storage::Chunk_Ptr &ptr)override {
		readed = ptr;
	}
};

InnerReader::InnerReader(dariadb::Flag flag, dariadb::Time from, dariadb::Time to) :
	_chunks{},
	_flag(flag),
	_from(from),
	_to(to),
	_tp_readed(false)
{
	is_time_point_reader = false;
	end = false;
}

void InnerReader::add(Cursor_ptr c) {
    std::lock_guard<dariadb::utils::Locker> lg(_locker);
    this->_chunks.push_back(c);
}

void InnerReader::add_tp(Chunk_Ptr c) {
    std::lock_guard<dariadb::utils::Locker> lg(_locker);
    this->_tp_chunks[c->first.id].push_back(c);
}

bool InnerReader::isEnd() const {
	return this->end && this->_tp_readed;
}

dariadb::IdArray InnerReader::getIds()const {

	dariadb::IdSet idset;
	for (auto &v : _chunks) {
		//TODO optimise
		dariadb::storage::ChuncksList out;
		v->readAll(&out);
		for (auto &c : out) {
			idset.insert(c->first.id);
		}
	}
	dariadb::IdArray result{ idset.begin(),idset.end() };
	return result;
}

void InnerReader::readNext(storage::ReaderClb*clb) {
    std::lock_guard<dariadb::utils::Locker> lg(_locker);

	if (!_tp_readed) {
        this->readTimePoint(clb);
	}
	std::shared_ptr<CursorReader> reader_clbk{ new CursorReader };
    for (auto ch : _chunks) {

        //for (Chunk_Ptr cur_ch:ch.second) 
		while(!ch->is_end())
		{
			ch->readNext(reader_clbk.get());
			if (reader_clbk->readed == nullptr) {
				break;
			}
			auto cur_ch = reader_clbk->readed;
            auto bw = std::make_shared<BinaryBuffer>(cur_ch->bw->get_range());
            bw->reset_pos();
            CopmressedReader crr(bw, cur_ch->first);
			
            if (check_meas(cur_ch->first)) {
                auto sub = cur_ch->first;
                clb->call(sub);
            }

            for (size_t j = 0; j < cur_ch->count; j++) {
                auto sub = crr.read();
                sub.id = cur_ch->first.id;
                if (check_meas(sub)) {
                    clb->call(sub);
                }
                else {
                    if (sub.time > _to) {
                        break;
                    }
                }
            }
        }
    }
	end = true;
}

void InnerReader::readTimePoint(storage::ReaderClb*clb) {
    std::lock_guard<dariadb::utils::Locker> lg(_locker_tp);
    std::list<Chunk_Ptr> to_read_chunks{};
	for (auto cand_ch : _tp_chunks) {
		auto candidate = cand_ch.second.front();

        for (auto cur_chunk: cand_ch.second) {
            if (candidate->first.time < cur_chunk->first.time) {
                candidate = cur_chunk;
			}
		}
		to_read_chunks.push_back(candidate);
	}

	for (auto ch : to_read_chunks) {

        auto bw = std::make_shared<BinaryBuffer>(ch->bw->get_range());
		bw->reset_pos();
        CopmressedReader crr(bw, ch->first);

		Meas candidate;
        candidate = ch->first;
        ch->lock();
        for (size_t i = 0; i < ch->count; i++) {

			auto sub = crr.read();
            sub.id = ch->first.id;
			if ((sub.time <= _from) && (sub.time >= candidate.time)) {
				candidate = sub;
			}if (sub.time > _from) {
				break;
			}
		}
        ch->unlock();
		if (candidate.time <= _from) {
			//TODO make as options
			candidate.time = _from;

			clb->call(candidate);
			_tp_readed_times.insert(std::make_tuple(candidate.id, candidate.time));
		}
	}
	auto m = dariadb::Meas::empty();
	m.time = _from;
	m.flag = dariadb::Flags::NO_DATA;
	for (auto id : _not_exist) {
		m.id = id;
		clb->call(m);
	}
	_tp_readed = true;
}


bool InnerReader::check_meas(const Meas&m)const {
	auto tmp = std::make_tuple(m.id, m.time);
	if (this->_tp_readed_times.find(tmp) != _tp_readed_times.end()) {
		return false;
	}
	using utils::inInterval;

	if ((in_filter(_flag, m.flag)) && (inInterval(_from, _to, m.time))) {
		return true;
	}
	return false;
}

Reader_ptr InnerReader::clone()const {
	auto res = std::make_shared<InnerReader>(_flag, _from, _to);
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
void InnerReader::reset() {
	end = false;
	_tp_readed = false;
	_tp_readed_times.clear();
	for (auto ch : _chunks) {
		ch->reset_pos();
	}
}

InnerCurrentValuesReader::InnerCurrentValuesReader() {
	this->end = false;
}
InnerCurrentValuesReader::~InnerCurrentValuesReader() {}

bool InnerCurrentValuesReader::isEnd() const {
	return this->end;
}

void InnerCurrentValuesReader::readCurVals(storage::ReaderClb*clb) {
	for (auto v : _cur_values) {
		clb->call(v);
	}
}

void InnerCurrentValuesReader::readNext(storage::ReaderClb*clb) {
    std::lock_guard<dariadb::utils::Locker> lg(_locker);
	readCurVals(clb);
	this->end = true;
}

IdArray InnerCurrentValuesReader::getIds()const {
	dariadb::IdArray result;
	result.resize(_cur_values.size());
	size_t pos = 0;
	for (auto v : _cur_values) {
		result[pos] = v.id;
		pos++;
	}
	return result;
}
Reader_ptr InnerCurrentValuesReader::clone()const {
	auto raw_reader = new InnerCurrentValuesReader();
	Reader_ptr result{ raw_reader };
	raw_reader->_cur_values = _cur_values;
	return result;
}
void InnerCurrentValuesReader::reset() {
	end = false;
}
