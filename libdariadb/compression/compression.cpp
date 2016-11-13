#include <libdariadb/compression/compression.h>

using namespace dariadb;
using namespace dariadb::compression;

CopmressedWriter::CopmressedWriter(const ByteBuffer_Ptr &bw)
	: _bb(bw), time_comp(bw), value_comp(bw), flag_comp(bw) {
	_is_first = true;
	_is_full = false;
}

CopmressedWriter::~CopmressedWriter() {}


bool CopmressedWriter::append(const Meas &m) {
	if (_is_first) {
		_first = m;
		_is_first = false;
	}

	auto t_f = time_comp.append(m.time);
	auto f_f = value_comp.append(m.value);
	auto v_f = flag_comp.append(m.flag);

	if (!t_f || !f_f || !v_f) {
		_is_full = true;
		return false;
	}
	else {
		return true;
	}
}

CopmressedReader::CopmressedReader(const ByteBuffer_Ptr &bw, const Meas &first) : time_dcomp(bw, first.time), value_dcomp(bw, first.value),
flag_dcomp(bw, first.flag) {
	_first = first;
}

CopmressedReader::~CopmressedReader() {}
