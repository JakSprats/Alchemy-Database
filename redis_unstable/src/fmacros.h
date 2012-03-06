#ifndef _REDIS_FMACRO_H
#define _REDIS_FMACRO_H

#ifndef _BSD_SOURCE /* ALCHEMY_DATABASE */
  #define _BSD_SOURCE
#endif

#ifdef __linux__
#define _XOPEN_SOURCE 700
#else
#define _XOPEN_SOURCE
#endif

#ifndef _LARGEFILE_SOURCE /* ALCHEMY_DATABASE */
  #define _LARGEFILE_SOURCE
#endif
#ifndef _FILE_OFFSET_BITS /* ALCHEMY_DATABASE */
  #define _FILE_OFFSET_BITS 64
#endif

#endif
