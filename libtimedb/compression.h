#pragma once

#include "meas.h"

namespace timedb {
	namespace compression {
		class BinaryBuffer {
			typedef uint8_t value_type;
		public:
			
			BinaryBuffer(size_t size);
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

			void incbit();
			void incpos();
			
			size_t cap()const { return _cap;}

			uint8_t getbit()const;
			void setbit();
			void clrbit();
		private:
			value_type* _buf;
			size_t   _cap;

			size_t _pos;
			int _bitnum;
		};

		class DeltaCompressor
		{
		public:
			DeltaCompressor();
			~DeltaCompressor();

			static uint16_t get_delta_64(uint64_t D);
			static uint16_t get_delta_256(uint64_t D);
			static uint16_t get_delta_2048(uint64_t D);
			static uint64_t get_delta_big(uint64_t D);
		};
	}
}