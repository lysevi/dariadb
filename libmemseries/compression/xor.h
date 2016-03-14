#pragma once

#include "base_compressor.h"

namespace memseries {
    namespace compression {
        namespace inner{
            union conv {
                double d;
                int64_t i;
            };

            inline int64_t flat_double_to_int(double v) {
                conv c;
                c.d = v;
                return c.i;
            }
            inline double flat_int_to_double(int64_t v) {
                conv c;
                c.i = v;
                return c.d;
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
