#include <common/queryes.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/exception.h>

void dariadb::net::send_meases(DataSender* con, const MeasArray &ma) {
	auto cur_id = con->getQueryId();

	auto byId = splitById(ma);
	dariadb::net::messages::QueryHeader qhdr;
	qhdr.set_id(cur_id);
	qhdr.set_kind(dariadb::net::messages::QueryKind::APPEND);

	dariadb::net::messages::QueryAppend *qap = qhdr.MutableExtension(dariadb::net::messages::QueryAppend::qappend);
	size_t total_count = 0;
	size_t count_to_write = 0;
	for (auto kv : byId) {
		auto var = qap->add_values();
		var->set_id(kv.first);
		for (auto v : kv.second) {
			count_to_write++;
			auto data = var->add_data();
			data->set_flag(v.flag);
			data->set_time(v.time);
			data->set_value(v.value);
			auto total_size = size_t(qhdr.ByteSize());
			if (total_size >= size_t(NetData::MAX_MESSAGE_SIZE * 0.75)) {
				logger_info("client: pack count: ", count_to_write);
				auto nd = con->makeNetData();
				nd->size = NetData::MAX_MESSAGE_SIZE - 1;
				if (!qhdr.SerializeToArray(nd->data, nd->size)) {
					THROW_EXCEPTION("append message serialize error");
				}

				nd->size = qhdr.ByteSize();

				con->sendData(nd, cur_id, dariadb::net::messages::QueryKind::APPEND);
				qhdr.Clear();

				cur_id = con->getQueryId();

				qhdr.set_id(cur_id);
				qhdr.set_kind(dariadb::net::messages::QueryKind::APPEND);
				qap = qhdr.MutableExtension(dariadb::net::messages::QueryAppend::qappend);
				total_count += count_to_write;
				count_to_write = 0;
			}
		}
	}

	if (count_to_write > 0) {
		logger_info("client: pack count: ", count_to_write);
		auto nd = con->makeNetData();
		nd->size = NetData::MAX_MESSAGE_SIZE - 1;
		if (!qhdr.SerializeToArray(nd->data, nd->size)) {
			THROW_EXCEPTION("append message serialize error");
		}
		nd->size = qhdr.ByteSize();
		con->sendData(nd, cur_id, dariadb::net::messages::QueryKind::APPEND);
		total_count += count_to_write;
	}
}

//TODO use callback
dariadb::net::ReadedValues dariadb::net::read_values(const NetData_ptr&nd) {
	auto hdr = nd->readHeader();
	dariadb::net::messages::QueryHeader *qhdr = (dariadb::net::messages::QueryHeader*)hdr.parsed_info;
	dariadb::net::messages::QueryAppend qap = qhdr->GetExtension(dariadb::net::messages::QueryAppend::qappend);
	auto count = qap.values_size();


	auto bg = qap.values().begin();
	auto end = qap.values().end();
	ReadedValues result;
	result.id = qhdr->id();
	for (auto it = bg; it != end; ++it) {
		auto va = *it;
		for (auto m_it = va.data().begin(); m_it != va.data().end(); ++m_it) {
			dariadb::Meas m;
			m.id = va.id();
			m.time = m_it->time();
			m.flag = m_it->flag();
			m.value = m_it->value();

			result.values.push_back(m);
		}
	}
	return result;
}