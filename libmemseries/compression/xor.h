#pragma once

#include "base_compressor.h"

namespace memseries {
    namespace compression {
        namespace inner{
            inline int64_t flat_double_to_int(double v) {
				return *(reinterpret_cast<int64_t*>(&v));
            }
            inline double flat_int_to_double(int64_t v) {
				return *(reinterpret_cast<double*>(&v));
            }
        }

        class XorCompressor:public BaseCompressor {
        public:
            XorCompressor() = default;
            XorCompressor(const BinaryBuffer &bw);
            ~XorCompressor();

            bool append(Value v);
            static uint8_t zeros_lead(uint64_t v);
            static uint8_t zeros_tail(uint64_t v);
        protected:
            bool _is_first;
            uint64_t _first;
            uint64_t _prev_value;
            uint8_t _prev_lead;
            uint8_t _prev_tail;
        };

        class XorDeCompressor:public BaseCompressor {
        public:
            XorDeCompressor() = default;
            XorDeCompressor(const BinaryBuffer &bw, Value first);
            ~XorDeCompressor() = default;

            Value read();
        protected:
            uint64_t _prev_value;
            uint8_t _prev_lead;
            uint8_t _prev_tail;
        };
    }
}
