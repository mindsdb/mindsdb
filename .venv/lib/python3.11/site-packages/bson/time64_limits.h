/*
   Maximum and minimum inputs your system's respective time functions
   can correctly handle.  time64.h will use your system functions if
   the input falls inside these ranges and corresponding USE_SYSTEM_*
   constant is defined.
*/

#ifndef TIME64_LIMITS_H
#define TIME64_LIMITS_H

/* Max/min for localtime() */
#define SYSTEM_LOCALTIME_MAX     2147483647
#define SYSTEM_LOCALTIME_MIN    -2147483647-1

/* Max/min for gmtime() */
#define SYSTEM_GMTIME_MAX        2147483647
#define SYSTEM_GMTIME_MIN       -2147483647-1

/* Max/min for mktime() */
static const struct tm SYSTEM_MKTIME_MAX = {
    7,
    14,
    19,
    18,
    0,
    138,
    1,
    17,
    0
#ifdef HAS_TM_TM_GMTOFF
    ,-28800
#endif
#ifdef HAS_TM_TM_ZONE
    ,"PST"
#endif
};

static const struct tm SYSTEM_MKTIME_MIN = {
    52,
    45,
    12,
    13,
    11,
    1,
    5,
    346,
    0
#ifdef HAS_TM_TM_GMTOFF
    ,-28800
#endif
#ifdef HAS_TM_TM_ZONE
    ,"PST"
#endif
};

/* Max/min for timegm() */
#ifdef HAS_TIMEGM
static const struct tm SYSTEM_TIMEGM_MAX = {
    7,
    14,
    3,
    19,
    0,
    138,
    2,
    18,
    0
    #ifdef HAS_TM_TM_GMTOFF
        ,0
    #endif
    #ifdef HAS_TM_TM_ZONE
        ,"UTC"
    #endif
};

static const struct tm SYSTEM_TIMEGM_MIN = {
    52,
    45,
    20,
    13,
    11,
    1,
    5,
    346,
    0
    #ifdef HAS_TM_TM_GMTOFF
        ,0
    #endif
    #ifdef HAS_TM_TM_ZONE
        ,"UTC"
    #endif
};
#endif /* HAS_TIMEGM */

#endif /* TIME64_LIMITS_H */
