#pragma once

#include <cstdint>

#ifdef MSVC
#include <intrin.h>
#include <windows.h>
uint8_t __inline ctz(uint64_t value)
{
	DWORD trailing_zero = 0;

	if (_BitScanForward64(&trailing_zero, value)){
		return static_cast<uint8_t>(trailing_zero);
	}
	else{
		return uint8_t(64);
	}
}

uint8_t __inline clz(uint64_t value)
{
	DWORD leading_zero = 0;

	if (_BitScanReverse64(&leading_zero, value)){
		return static_cast<uint8_t>(63 - leading_zero);
	}
	else{
		return uint8_t(64);
	}
}
#elif defined(GNU_CPP) || defined(CLANG_CPP)
#define clz(x) static_cast<uint8_t>(__builtin_clzll(x))
#define ctz(x) static_cast<uint8_t>(__builtin_ctzll(x))
#else
static uint8_t inline clz(uint64_t v)
{
	const int value_max_bit_pos = sizeof(dariadb::Value) * 8 - 1;
	uint8_t result = 0;
	for (int8_t i = value_max_bit_pos; i >= 0; i -= 2) {
		if (dariadb::utils::BitOperations::check(v, i)) {
			break;
		}
		else {
			result++;
		}

		if (dariadb::utils::BitOperations::check(v, i - 1)) {
			break;
		}
		else {
			result++;
		}
	}
	return result;
}
static uint8_t inline ctz(uint64_t v)
{
	const int value_max_bit_pos = sizeof(dariadb::Value) * 8 - 1;
	uint8_t result = 0;
	for (int8_t i = 0; i<value_max_bit_pos; i += 2) {
		if (dariadb::utils::BitOperations::check(v, i)) {
			break;
		}
		else {
			result++;
		}

		if (dariadb::utils::BitOperations::check(v, i + 1)) {
			break;
		}
		else {
			result++;
		}
	}
	return result;
}

#endif