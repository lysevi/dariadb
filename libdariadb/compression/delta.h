#pragma once

#include "base_compressor.h"
#include "positions.h"

namespace dariadb {
    namespace compression {

        class DeltaCompressor:public BaseCompressor {
        public:
            DeltaCompressor() = default;
            DeltaCompressor(const BinaryBuffer_Ptr &bw);
            ~DeltaCompressor();

            bool append(Time t);

            DeltaCompressionPosition get_position()const;
            void restore_position(const DeltaCompressionPosition&pos);

            static uint16_t get_delta_64(int64_t D);
            static uint16_t get_delta_256(int64_t D);
            static uint16_t get_delta_2048(int64_t D);
            static uint64_t get_delta_big(int64_t D);

        protected:
            bool _is_first;
            Time _first;
            int64_t _prev_delta;
            Time _prev_time;
        };

        class DeltaDeCompressor:public BaseCompressor {
        public:
            DeltaDeCompressor() = default;
            DeltaDeCompressor(const BinaryBuffer_Ptr &bw, Time first);
            ~DeltaDeCompressor();

            Time read();
        protected:
            int64_t _prev_delta;
            Time _prev_time;
        };
    }
}
