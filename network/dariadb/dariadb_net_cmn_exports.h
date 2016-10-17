
#ifndef DARIADBNET_CMN_EXPORTS_H
#define DARIADBNET_CMN_EXPORTS_H

#ifdef SHARED_EXPORTS_BUILT_AS_STATIC
#  define DARIADBNET_CMN_EXPORTS
#  define LIBCOMMON_NET_NO_EXPORT
#else
#  ifndef DARIADBNET_CMN_EXPORTS
#    ifdef libcommon_net_EXPORTS
        /* We are building this library */
#      define DARIADBNET_CMN_EXPORTS __declspec(dllexport)
#    else
        /* We are using this library */
#      define DARIADBNET_CMN_EXPORTS __declspec(dllimport)
#    endif
#  endif

#  ifndef LIBCOMMON_NET_NO_EXPORT
#    define LIBCOMMON_NET_NO_EXPORT 
#  endif
#endif

#ifndef LIBCOMMON_NET_DEPRECATED
#  define LIBCOMMON_NET_DEPRECATED __declspec(deprecated)
#endif

#ifndef LIBCOMMON_NET_DEPRECATED_EXPORT
#  define LIBCOMMON_NET_DEPRECATED_EXPORT DARIADBNET_CMN_EXPORTS LIBCOMMON_NET_DEPRECATED
#endif

#ifndef LIBCOMMON_NET_DEPRECATED_NO_EXPORT
#  define LIBCOMMON_NET_DEPRECATED_NO_EXPORT LIBCOMMON_NET_NO_EXPORT LIBCOMMON_NET_DEPRECATED
#endif

#define DEFINE_NO_DEPRECATED 0
#if DEFINE_NO_DEPRECATED
# define LIBCOMMON_NET_NO_DEPRECATED
#endif

#endif
