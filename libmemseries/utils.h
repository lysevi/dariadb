#pragma once

#include <string>
#include <list>
#include <iterator>

#define NOT_IMPLEMENTED throw std::logic_error("Not implemented");

#ifdef _DEBUG
#define ENSURE(A, E)                                                           \
    if (!(A)) {                                                                  \
    throw std::invalid_argument(E);                                            \
    }
#define ENSURE_NOT_NULL(A) ENSURE(A, "null pointer")
#else
#define ENSURE(A, E)
#define ENSURE_NOT_NULL(A)
#endif

namespace memseries {
	namespace utils
	{

		struct BitOperations {
            template<class T>
            static inline uint8_t get(T v, uint8_t num) {
				return (v >> num) & 1;
			}

            template<class T>
            static inline bool check(T v, uint8_t num) {
				return get(v, num) == 1;
			}

            template<class T>
            static inline T set(T v, uint8_t num) {
                return v | (T(1) << num);
            }

            template<class T>
            static inline T clr(T v, uint8_t num) {
                return v & ~(T(1) << num);
            }
		};

		class NonCopy {
		private:
			NonCopy(const NonCopy &) = delete;
			NonCopy &operator=(const NonCopy &) = delete;

		protected:
			NonCopy() = default;
		};

		template <typename T> bool inInterval(T from, T to, T value) {
			return value >= from && value <= to;
		}
	}
}
