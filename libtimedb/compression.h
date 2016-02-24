#pragma once

#include <ostream>
#include "meas.h"

namespace timedb {
	namespace compression {

        const uint8_t max_bit_pos = 7;

        class BinaryBuffer {
            
		public:
            BinaryBuffer(uint8_t* _begin,uint8_t*_end);
            BinaryBuffer(const BinaryBuffer&other);
            BinaryBuffer(BinaryBuffer&&other);
            ~BinaryBuffer();
            BinaryBuffer& operator=(const BinaryBuffer&other);

            void swap(BinaryBuffer &other) throw();

            int bitnum()const { return _bitnum; }
			size_t pos()const { return _pos; }
			
			void set_bitnum(size_t num);
			void set_pos(size_t pos);
			void reset_pos();

            BinaryBuffer& incbit();
            BinaryBuffer& incpos();
			
			size_t cap()const { return _cap;}

			uint8_t getbit()const;
            BinaryBuffer& setbit();
            BinaryBuffer& clrbit();
			
			friend std::ostream& operator<< (std::ostream& stream, const BinaryBuffer& b);
        protected:
            uint8_t* _begin,*_end;
			size_t   _cap;

			size_t _pos;
            int _bitnum;
		};

		std::ostream& operator<< (std::ostream& stream, const BinaryBuffer& b);

        class DeltaCompressor
		{
		public:
            DeltaCompressor()=default;
            DeltaCompressor(const BinaryBuffer &bw);
			~DeltaCompressor();

            void append(Time t);

            static uint16_t get_delta_64(int64_t D);
            static uint16_t get_delta_256(int64_t D);
            static uint16_t get_delta_2048(int64_t D);
            static uint64_t get_delta_big(int64_t D);
        protected:
            bool _is_first;
            BinaryBuffer _bw;
            Time  _first;
            uint64_t _prev_delta;
            Time _prev_time;
		};

        class DeltaDeCompressor
        {
        public:
            DeltaDeCompressor()=default;
            DeltaDeCompressor(const BinaryBuffer &bw, Time first);
            ~DeltaDeCompressor();

            Time read();
        protected:
            BinaryBuffer _bw;
            uint64_t _prev_delta;
            Time _prev_time;
        };

        class XorCompressor
        {
        public:
            XorCompressor()=default;
            XorCompressor(const BinaryBuffer &bw);
            ~XorCompressor();

            void append(Value v);
            static uint8_t zeros_lead(Value v);
            static uint8_t zeros_tail(Value v);
        protected:
            bool _is_first;
            BinaryBuffer _bw;
            Value  _first;
            Value _prev_value;
            uint8_t _prev_lead;
            uint8_t _prev_tail;
        };


        class XorDeCompressor
        {
        public:
            XorDeCompressor()=default;
            XorDeCompressor(const BinaryBuffer &bw, Value first);
            ~XorDeCompressor()=default;

            Value read();
        protected:
            BinaryBuffer _bw;
            Value _prev_value;
            uint8_t _prev_lead;
            uint8_t _prev_tail;
        };
    }
}
