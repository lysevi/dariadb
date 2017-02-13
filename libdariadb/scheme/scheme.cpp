#include <libdariadb/scheme/scheme.h>
#include <libdariadb/scheme/helpers.h>

using namespace dariadb;
using namespace dariadb::scheme;

struct Scheme::Private {
  Private(const storage::Manifest_ptr m) {}

  storage::Manifest_ptr _manifest;
};

Scheme::Scheme(const storage::Manifest_ptr m) : _impl(new Scheme::Private(m)) {}