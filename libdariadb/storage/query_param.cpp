#include "query_param.h"
#include <json/json.hpp>

namespace dariadb {
	namespace storage {
		nlohmann::json to_json(IdArray ids, Flag flag, Flag source) {
			nlohmann::json res;
			res["flag"] = flag;
			res["source"] = source;
			res["ids"] = ids;
			return res;
		}
		std::string QueryParam::to_string() const {
			return to_json(ids,flag,source).dump();
		}

		void QueryParam::from_string(const std::string&str) {
			auto js = nlohmann::json::parse(str);
			flag = js["flag"];
			source = js["source"];
			auto ids_js = js["ids"];
			assert(ids_js.is_array());
			for (auto v : ids_js) {
				dariadb::Id id = v;
				this->ids.push_back(id);
			}
		}

		std::string QueryInterval::to_string() const {
			auto sub_res=to_json(ids, flag, source);
			sub_res["from"] = from;
			sub_res["to"] = to;
			return sub_res.dump();
		}

		void QueryInterval::from_string(const std::string&str) {
			auto js = nlohmann::json::parse(str);
			flag = js["flag"];
			source = js["source"];
			from=js["from"];
			to=js["to"];
			auto ids_js = js["ids"];
			assert(ids_js.is_array());
			for (auto v : ids_js) {
				dariadb::Id id = v;
				this->ids.push_back(id);
			}
		}

		std::string QueryTimePoint::to_string() const {
			auto sub_res = to_json(ids, flag, source);
			sub_res["time_point"] = time_point;
			return sub_res.dump();
		}

		void QueryTimePoint::from_string(const std::string&str) {
			auto js = nlohmann::json::parse(str);
			flag = js["flag"];
			source = js["source"];
			time_point = js["time_point"];
			auto ids_js = js["ids"];
			assert(ids_js.is_array());
			for (auto v : ids_js) {
				dariadb::Id id = v;
				this->ids.push_back(id);
			}
		}
	}
}