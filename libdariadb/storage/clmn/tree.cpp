#include <libdariadb/storage/clmn/tree.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::storage::clmn;
namespace clmn_inner {
/// create chunk and pack measurements.
Chunk_Ptr measArrayToChunk(const MeasArray &ma, size_t ma_size, uint8_t *buffer,
                           uint32_t buffer_size) {
  ENSURE(ma.front().time <= ma.back().time);
  ENSURE(ma_size != 0);

  ChunkHeader *hdr = new ChunkHeader();

  auto pos = size_t(0);
  auto chunk = Chunk::create(hdr, buffer, buffer_size, ma[pos]);
  chunk->is_owner = true;
  ++pos;

  for (; pos < ma_size; ++pos) {
    chunk->append(ma[pos]);
  }
  chunk->close();
  return chunk;
}
}
struct Tree::Private {
  struct LeafData {
    typedef std::shared_ptr<LeafData> Ptr;

    MeasArray a;
    size_t write_position;

    LeafData(node_sz_t sz) : a(size_t(sz)), write_position(0) {}

    static Ptr create(node_sz_t sz) { return Ptr{new LeafData(sz)}; }

    bool append(const Meas &m) {
      if (write_position < a.size()) {
        a[write_position++] = m;
        return true;
      } else {
        return false;
      }
    }
  };

  Private(const NodeStorage_Ptr &nstore, const Params &params)
      : _nstore(nstore), _params(params) {
    ENSURE(_params.measurementsInLeaf > 0);
    ENSURE(_params.nodeChildren > 0);

    _buffer_size =
        static_cast<uint32_t>(sizeof(Meas) * _params.measurementsInLeaf);
    _buffer = new uint8_t[_buffer_size];
  }

  void append(const Meas &m) {
    auto fres = _leafs_data.find(m.id);
    if (fres == _leafs_data.end()) {
      LeafData::Ptr ld = LeafData::create(_params.measurementsInLeaf);
      ld->append(m);
      _leafs_data.insert(std::make_pair(m.id, ld));
      return;
    } else {

      auto result = fres->second->append(m);

      if (!result) {
        writeTree(fres->second);
        append(m);
      }
    }
  }

  void writeTree(const LeafData::Ptr &ld) {
    ENSURE(ld->write_position != 0);
    std::sort(ld->a.begin(), ld->a.begin() + ld->write_position,
              meas_time_compare_less());

    std::fill_n(_buffer, _buffer_size, uint8_t(0));

    auto ch = clmn_inner::measArrayToChunk(ld->a, ld->write_position, _buffer,
                                           _buffer_size);
    auto skip_count = Chunk::compact(ch->header);
    Chunk::updateChecksum(*ch->header, ch->_buffer_t + skip_count);
    // auto buff=ch->_buffer_t + skip_count;
    // auto sz=ch->header->size

    Leaf::Ptr l = Leaf::make_leaf(gnrt_t(0), ch->header->size);
    l->chunk_hdr = *ch->header;
    for (size_t i = 0; i < ch->header->size; ++i) {
      l->chunk_buffer[i] = ch->_buffer_t[skip_count + i];
    }

    auto addr = _nstore->write_leaf(l);
    ENSURE(addr != NODE_PTR_NULL);

    ld->write_position = 0;
  }

  NodeStorage_Ptr _nstore;
  Params _params;

  uint32_t _buffer_size;
  uint8_t *_buffer;
  // TODO replace MeasArray to Chunk.
  std::unordered_map<Id, LeafData::Ptr> _leafs_data;
};

Tree::Ptr Tree::create(const NodeStorage_Ptr &nstore, const Params &params) {
  return Tree::Ptr{new Tree(nstore, params)};
}

Tree::Tree(const NodeStorage_Ptr &nstore, const Params &params)
    : _impl{new Tree::Private(nstore, params)} {}

Tree::~Tree() { _impl = nullptr; }

void Tree::append(const Meas &m) { return _impl->append(m); }