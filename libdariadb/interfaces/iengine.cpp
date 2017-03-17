#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/utils/async/thread_manager.h>

using namespace dariadb;
using namespace dariadb::utils::async;

void IEngine::foreach (const QueryInterval &q, IReadCallback * clbk) {
  auto params = std::make_shared<foreach_async_data>(q, clbk);

  AsyncTask at_t = [this, params](const ThreadInfo &ti) {
    TKIND_CHECK(THREAD_KINDS::COMMON, ti.kind);

    if (params->next_id_pos < params->q.ids.size()) {
      if (!params->clbk->is_canceled()) {
        // auto start_time = clock();

        QueryInterval local_q = params->q;
        local_q.ids.resize(1);
        auto id = params->q.ids[params->next_id_pos++];

        local_q.ids[0] = id;
        auto r = intervalReader(local_q);
        auto fres = r.find(id);
        if (fres != r.end()) {
          fres->second->apply(params->clbk, local_q);
        }
        /*auto elapsed_time = (((float)clock() - start_time) / CLOCKS_PER_SEC);
        logger("foreach: #", id, ": elapsed ", elapsed_time, "s");*/

        if (params->next_id_pos < params->q.ids.size()) {
          return true;
        }
      }
    }
    params->clbk->is_end();
    return false;
  };

  auto at = ThreadManager::instance()->post(THREAD_KINDS::COMMON, AT(at_t));
}

void IEngine::foreach (const QueryTimePoint &q, IReadCallback * clbk) {
  auto values = this->readTimePoint(q);
  for (auto &kv : values) {
    if (clbk->is_canceled()) {
      break;
    }
    clbk->apply(kv.second);
  }
  clbk->is_end();
}
