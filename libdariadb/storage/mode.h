#pragma once

#include <cstdint>

namespace dariadb {
    namespace storage {
        ///
        /// \brief The STORAGE_MODE enum
        /// method write when page is fulle
        enum class MODE : uint8_t {
            SINGLE ///single file mode. owerwrite old chunks.
        };
    }
}
