
#ifndef CL_EXPORT_H
#define CL_EXPORT_H

#ifdef SHARED_EXPORTS_BUILT_AS_STATIC
#  define CL_EXPORT
#  define LIBDARIADB_CL_NO_EXPORT
#else
#  ifndef CL_EXPORT
#    ifdef libdariadb_cl_EXPORTS
        /* We are building this library */
#      define CL_EXPORT __declspec(dllexport)
#    else
        /* We are using this library */
#      define CL_EXPORT __declspec(dllimport)
#    endif
#  endif

#  ifndef LIBDARIADB_CL_NO_EXPORT
#    define LIBDARIADB_CL_NO_EXPORT 
#  endif
#endif

#ifndef LIBDARIADB_CL_DEPRECATED
#  define LIBDARIADB_CL_DEPRECATED __declspec(deprecated)
#endif

#ifndef LIBDARIADB_CL_DEPRECATED_EXPORT
#  define LIBDARIADB_CL_DEPRECATED_EXPORT CL_EXPORT LIBDARIADB_CL_DEPRECATED
#endif

#ifndef LIBDARIADB_CL_DEPRECATED_NO_EXPORT
#  define LIBDARIADB_CL_DEPRECATED_NO_EXPORT LIBDARIADB_CL_NO_EXPORT LIBDARIADB_CL_DEPRECATED
#endif

#define DEFINE_NO_DEPRECATED 0
#if DEFINE_NO_DEPRECATED
# define LIBDARIADB_CL_NO_DEPRECATED
#endif

#endif
