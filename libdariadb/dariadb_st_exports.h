
#ifndef DARIADB_ST_EXPORTS_H
#define DARIADB_ST_EXPORTS_H

#ifdef SHARED_EXPORTS_BUILT_AS_STATIC
#  define DARIADB_ST_EXPORTS
#  define LIBDARIADB_NO_EXPORT
#else
#  ifndef DARIADB_ST_EXPORTS
#    ifdef libdariadb_EXPORTS
        /* We are building this library */
#      define DARIADB_ST_EXPORTS __declspec(dllexport)
#    else
        /* We are using this library */
#      define DARIADB_ST_EXPORTS __declspec(dllimport)
#    endif
#  endif

#  ifndef LIBDARIADB_NO_EXPORT
#    define LIBDARIADB_NO_EXPORT 
#  endif
#endif

#ifndef LIBDARIADB_DEPRECATED
#  define LIBDARIADB_DEPRECATED 
#endif

#ifndef LIBDARIADB_DEPRECATED_EXPORT
#  define LIBDARIADB_DEPRECATED_EXPORT DARIADB_ST_EXPORTS LIBDARIADB_DEPRECATED
#endif

#ifndef LIBDARIADB_DEPRECATED_NO_EXPORT
#  define LIBDARIADB_DEPRECATED_NO_EXPORT LIBDARIADB_NO_EXPORT LIBDARIADB_DEPRECATED
#endif

#define DEFINE_NO_DEPRECATED 0
#if DEFINE_NO_DEPRECATED
# define LIBDARIADB_NO_DEPRECATED
#endif

#endif
