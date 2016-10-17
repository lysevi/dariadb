#pragma once

#include <common/net_data.h>
#include <libdariadb/meas.h>

#include <common/dariadb_net_cmn_exports.h>

namespace dariadb {
namespace net {
class DataSender {
public:
	virtual void sendData(NetData_ptr&nd, const QueryNumber id, dariadb::net::messages::QueryKind kind) = 0;
	virtual QueryNumber getQueryId()=0;
	virtual NetData_ptr makeNetData() = 0;
};
DARIADBNET_CMN_EXPORTS void send_meases(DataSender* con,const MeasArray &ma);
struct ReadedValues {
	MeasList values;
	QueryNumber id;
};
DARIADBNET_CMN_EXPORTS ReadedValues read_values(const NetData_ptr&nd);
};
};