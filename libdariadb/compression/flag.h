#pragma once

#include "base_compressor.h"
#include "positions.h"

namespace dariadb {
    namespace compression {

        class FlagCompressor:public BaseCompressor {
        public:
            FlagCompressor(const BinaryBuffer_Ptr &bw);

            bool append(Flag v);

            FlagCompressionPosition get_position()const;
            void restore_position(const FlagCompressionPosition&pos);
        protected:
            bool _is_first;
            Flag _first;
        };

        class FlagDeCompressor :public BaseCompressor {
        public:
            FlagDeCompressor(const BinaryBuffer_Ptr &bw, Flag first);

            Flag read();
        protected:
            Flag _prev_value;
        };

    }
}
