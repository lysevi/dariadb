#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <libdariadb/storage/clmn/nodes.h>
#include <libdariadb/utils/fs.h>

#include <boost/test/unit_test.hpp>
using namespace dariadb::storage;

BOOST_AUTO_TEST_CASE(Footer) {
  const char *expected = "dariadb.";
  auto number_representation = clmn::MAGIC_NUMBER;
  char *magic_str = (char *)&number_representation;
  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    BOOST_CHECK_EQUAL(magic_str[i], expected[i]);
  }

  const clmn::node_size_t expected_s = 11;

  clmn::Footer::Footer_Ptr ftr = clmn::Footer::make_footer(expected_s);
  BOOST_CHECK_EQUAL(ftr->crc, uint32_t(0));
  BOOST_CHECK_EQUAL(ftr->magic_num, clmn::MAGIC_NUMBER);
  BOOST_CHECK_EQUAL(ftr->hdr.kind, clmn::NODE_KIND_FOOTER);
  BOOST_CHECK_EQUAL(ftr->hdr.size, uint32_t(expected_s));
  BOOST_CHECK_EQUAL(ftr->hdr.gen, clmn::generation_t(0));
  BOOST_CHECK_EQUAL(ftr->max_node_id(), clmn::node_id_t(0));

  for (clmn::node_size_t i = 0; i < expected_s; ++i) {
    BOOST_CHECK_EQUAL(ftr->roots[i], clmn::NODE_PTR_NULL);
  }

  for (clmn::node_size_t i = 0; i < expected_s; ++i) {
    BOOST_CHECK_EQUAL(ftr->ids[i], dariadb::MAX_ID);
  }
}

void checkNodeCtor(const clmn::Node::Node_Ptr &n, clmn::generation_t expected_g,
                   clmn::node_id_t expected_i, clmn::node_size_t expected_s) {

  BOOST_CHECK_EQUAL(n->neighbor, clmn::NODE_PTR_NULL);
  BOOST_CHECK_EQUAL(n->hdr.gen, expected_g);
  BOOST_CHECK_EQUAL(n->hdr.id, expected_i);
  BOOST_CHECK_EQUAL(n->hdr.size, expected_s);
  for (clmn::node_size_t i = 0; i < expected_s; ++i) {
    BOOST_CHECK_EQUAL(n->children[i], clmn::NODE_PTR_NULL);
  }
}

BOOST_AUTO_TEST_CASE(LeafAndNode) {
  const clmn::generation_t expected_g = 1;
  const clmn::node_id_t expected_i = 2;
  const clmn::node_size_t expected_s = 3;

  {
    clmn::Node::Node_Ptr r = clmn::Node::make_root(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(r->hdr.kind, clmn::NODE_KIND_ROOT);
    checkNodeCtor(r, expected_g, expected_i, expected_s);
  }

  {
    clmn::Node::Node_Ptr n = clmn::Node::make_node(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(n->hdr.kind, clmn::NODE_KIND_NODE);
    checkNodeCtor(n, expected_g, expected_i, expected_s);
  }

  {
    clmn::Leaf::Leaf_Ptr l = clmn::Leaf::make_leaf(expected_g, expected_i, expected_s);

    BOOST_CHECK_EQUAL(l->neighbor, clmn::NODE_PTR_NULL);
    BOOST_CHECK_EQUAL(l->hdr.kind, clmn::NODE_KIND_LEAF);
    BOOST_CHECK_EQUAL(l->hdr.gen, expected_g);
    BOOST_CHECK_EQUAL(l->hdr.id, expected_i);
    BOOST_CHECK_EQUAL(l->hdr.size, expected_s);
  }
}