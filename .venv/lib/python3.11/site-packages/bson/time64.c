/*

Copyright (c) 2007-2010  Michael G Schwern

This software originally derived from Paul Sheer's pivotal_gmtime_r.c.

The MIT License:

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/

/*

Programmers who have available to them 64-bit time values as a 'long
long' type can use cbson_localtime64_r() and cbson_gmtime64_r() which correctly
converts the time even on 32-bit systems. Whether you have 64-bit time
values will depend on the operating system.

cbson_localtime64_r() is a 64-bit equivalent of localtime_r().

cbson_gmtime64_r() is a 64-bit equivalent of gmtime_r().

*/

#ifdef _MSC_VER
    #define _CRT_SECURE_NO_WARNINGS
#endif

/* Including Python.h fixes issues with interpreters built with -std=c99. */
#define PY_SSIZE_T_CLEAN
#include "Python.h"

#include <time.h>
#include "time64.h"
#include "time64_limits.h"


/* Spec says except for stftime() and the _r() functions, these
   all return static memory.  Stabbings! */
static struct TM   Static_Return_Date;

static const int days_in_month[2][12] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
};

static const int julian_days_by_month[2][12] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335},
};

static const int length_of_year[2] = { 365, 366 };

/* Some numbers relating to the gregorian cycle */
static const Year     years_in_gregorian_cycle   = 400;
#define               days_in_gregorian_cycle      ((365 * 400) + 100 - 4 + 1)
static const Time64_T seconds_in_gregorian_cycle = days_in_gregorian_cycle * 60LL * 60LL * 24LL;

/* Year range we can trust the time functions with */
#define MAX_SAFE_YEAR 2037
#define MIN_SAFE_YEAR 1971

/* 28 year Julian calendar cycle */
#define SOLAR_CYCLE_LENGTH 28

/* Year cycle from MAX_SAFE_YEAR down. */
static const int safe_years_high[SOLAR_CYCLE_LENGTH] = {
    2016, 2017, 2018, 2019,
    2020, 2021, 2022, 2023,
    2024, 2025, 2026, 2027,
    2028, 2029, 2030, 2031,
    2032, 2033, 2034, 2035,
    2036, 2037, 2010, 2011,
    2012, 2013, 2014, 2015
};

/* Year cycle from MIN_SAFE_YEAR up */
static const int safe_years_low[SOLAR_CYCLE_LENGTH] = {
    1996, 1997, 1998, 1971,
    1972, 1973, 1974, 1975,
    1976, 1977, 1978, 1979,
    1980, 1981, 1982, 1983,
    1984, 1985, 1986, 1987,
    1988, 1989, 1990, 1991,
    1992, 1993, 1994, 1995,
};

/* Let's assume people are going to be looking for dates in the future.
   Let's provide some cheats so you can skip ahead.
   This has a 4x speed boost when near 2008.
*/
/* Number of days since epoch on Jan 1st, 2008 GMT */
#define CHEAT_DAYS  (1199145600 / 24 / 60 / 60)
#define CHEAT_YEARS 108

#define IS_LEAP(n)      ((!(((n) + 1900) % 400) || (!(((n) + 1900) % 4) && (((n) + 1900) % 100))) != 0)
#define _TIME64_WRAP(a,b,m)     ((a) = ((a) <  0  ) ? ((b)--, (a) + (m)) : (a))

#ifdef USE_SYSTEM_LOCALTIME
#    define SHOULD_USE_SYSTEM_LOCALTIME(a)  (       \
    (a) <= SYSTEM_LOCALTIME_MAX &&              \
    (a) >= SYSTEM_LOCALTIME_MIN                 \
)
#else
#    define SHOULD_USE_SYSTEM_LOCALTIME(a)      (0)
#endif

#ifdef USE_SYSTEM_GMTIME
#    define SHOULD_USE_SYSTEM_GMTIME(a)     (       \
    (a) <= SYSTEM_GMTIME_MAX    &&              \
    (a) >= SYSTEM_GMTIME_MIN                    \
)
#else
#    define SHOULD_USE_SYSTEM_GMTIME(a)         (0)
#endif

/* Multi varadic macros are a C99 thing, alas */
#ifdef TIME_64_DEBUG
#    define TIME64_TRACE(format) (fprintf(stderr, format))
#    define TIME64_TRACE1(format, var1)    (fprintf(stderr, format, var1))
#    define TIME64_TRACE2(format, var1, var2)    (fprintf(stderr, format, var1, var2))
#    define TIME64_TRACE3(format, var1, var2, var3)    (fprintf(stderr, format, var1, var2, var3))
#else
#    define TIME64_TRACE(format) ((void)0)
#    define TIME64_TRACE1(format, var1) ((void)0)
#    define TIME64_TRACE2(format, var1, var2) ((void)0)
#    define TIME64_TRACE3(format, var1, var2, var3) ((void)0)
#endif


static int is_exception_century(Year year)
{
    int is_exception = ((year % 100 == 0) && !(year % 400 == 0));
    TIME64_TRACE1("# is_exception_century: %s\n", is_exception ? "yes" : "no");

    return(is_exception);
}


/* Compare two dates.
   The result is like cmp.
   Ignores things like gmtoffset and dst
*/
int cbson_cmp_date( const struct TM* left, const struct tm* right ) {
    if( left->tm_year > right->tm_year )
        return 1;
    else if( left->tm_year < right->tm_year )
        return -1;

    if( left->tm_mon > right->tm_mon )
        return 1;
    else if( left->tm_mon < right->tm_mon )
        return -1;

    if( left->tm_mday > right->tm_mday )
        return 1;
    else if( left->tm_mday < right->tm_mday )
        return -1;

    if( left->tm_hour > right->tm_hour )
        return 1;
    else if( left->tm_hour < right->tm_hour )
        return -1;

    if( left->tm_min > right->tm_min )
        return 1;
    else if( left->tm_min < right->tm_min )
        return -1;

    if( left->tm_sec > right->tm_sec )
        return 1;
    else if( left->tm_sec < right->tm_sec )
        return -1;

    return 0;
}


/* Check if a date is safely inside a range.
   The intention is to check if its a few days inside.
*/
int cbson_date_in_safe_range( const struct TM* date, const struct tm* min, const struct tm* max ) {
    if( cbson_cmp_date(date, min) == -1 )
        return 0;

    if( cbson_cmp_date(date, max) == 1 )
        return 0;

    return 1;
}


/* timegm() is not in the C or POSIX spec, but it is such a useful
   extension I would be remiss in leaving it out.  Also I need it
   for cbson_localtime64()
*/
Time64_T cbson_timegm64(const struct TM *date) {
    Time64_T days    = 0;
    Time64_T seconds = 0;
    Year     year;
    Year     orig_year = (Year)date->tm_year;
    int      cycles  = 0;

    if( orig_year > 100 ) {
        cycles = (int)((orig_year - 100) / 400);
        orig_year -= cycles * 400;
        days      += (Time64_T)cycles * days_in_gregorian_cycle;
    }
    else if( orig_year < -300 ) {
        cycles = (int)((orig_year - 100) / 400);
        orig_year -= cycles * 400;
        days      += (Time64_T)cycles * days_in_gregorian_cycle;
    }
    TIME64_TRACE3("# timegm/ cycles: %d, days: %lld, orig_year: %lld\n", cycles, days, orig_year);

    if( orig_year > 70 ) {
        year = 70;
        while( year < orig_year ) {
            days += length_of_year[IS_LEAP(year)];
            year++;
        }
    }
    else if ( orig_year < 70 ) {
        year = 69;
        do {
            days -= length_of_year[IS_LEAP(year)];
            year--;
        } while( year >= orig_year );
    }

    days += julian_days_by_month[IS_LEAP(orig_year)][date->tm_mon];
    days += date->tm_mday - 1;

    seconds = days * 60 * 60 * 24;

    seconds += date->tm_hour * 60 * 60;
    seconds += date->tm_min * 60;
    seconds += date->tm_sec;

    return(seconds);
}


#ifndef NDEBUG
static int check_tm(struct TM *tm)
{
    /* Don't forget leap seconds */
    assert(tm->tm_sec >= 0);
    assert(tm->tm_sec <= 61);

    assert(tm->tm_min >= 0);
    assert(tm->tm_min <= 59);

    assert(tm->tm_hour >= 0);
    assert(tm->tm_hour <= 23);

    assert(tm->tm_mday >= 1);
    assert(tm->tm_mday <= days_in_month[IS_LEAP(tm->tm_year)][tm->tm_mon]);

    assert(tm->tm_mon  >= 0);
    assert(tm->tm_mon  <= 11);

    assert(tm->tm_wday >= 0);
    assert(tm->tm_wday <= 6);

    assert(tm->tm_yday >= 0);
    assert(tm->tm_yday <= length_of_year[IS_LEAP(tm->tm_year)]);

#ifdef HAS_TM_TM_GMTOFF
    assert(tm->tm_gmtoff >= -24 * 60 * 60);
    assert(tm->tm_gmtoff <=  24 * 60 * 60);
#endif

    return 1;
}
#endif


/* The exceptional centuries without leap years cause the cycle to
   shift by 16
*/
static Year cycle_offset(Year year)
{
    const Year start_year = 2000;
    Year year_diff  = year - start_year;
    Year exceptions;

    if( year > start_year )
        year_diff--;

    exceptions  = year_diff / 100;
    exceptions -= year_diff / 400;

    TIME64_TRACE3("# year: %lld, exceptions: %lld, year_diff: %lld\n",
          year, exceptions, year_diff);

    return exceptions * 16;
}

/* For a given year after 2038, pick the latest possible matching
   year in the 28 year calendar cycle.

   A matching year...
   1) Starts on the same day of the week.
   2) Has the same leap year status.

   This is so the calendars match up.

   Also the previous year must match.  When doing Jan 1st you might
   wind up on Dec 31st the previous year when doing a -UTC time zone.

   Finally, the next year must have the same start day of week.  This
   is for Dec 31st with a +UTC time zone.
   It doesn't need the same leap year status since we only care about
   January 1st.
*/
static int safe_year(const Year year)
{
    int safe_year = 0;
    Year year_cycle;

    if( year >= MIN_SAFE_YEAR && year <= MAX_SAFE_YEAR ) {
        return (int)year;
    }

    year_cycle = year + cycle_offset(year);

    /* safe_years_low is off from safe_years_high by 8 years */
    if( year < MIN_SAFE_YEAR )
        year_cycle -= 8;

    /* Change non-leap xx00 years to an equivalent */
    if( is_exception_century(year) )
        year_cycle += 11;

    /* Also xx01 years, since the previous year will be wrong */
    if( is_exception_century(year - 1) )
        year_cycle += 17;

    year_cycle %= SOLAR_CYCLE_LENGTH;
    if( year_cycle < 0 )
        year_cycle = SOLAR_CYCLE_LENGTH + year_cycle;

    assert( year_cycle >= 0 );
    assert( year_cycle < SOLAR_CYCLE_LENGTH );
    if( year < MIN_SAFE_YEAR )
        safe_year = safe_years_low[year_cycle];
    else if( year > MAX_SAFE_YEAR )
        safe_year = safe_years_high[year_cycle];
    else
        assert(0);

    TIME64_TRACE3("# year: %lld, year_cycle: %lld, safe_year: %d\n",
          year, year_cycle, safe_year);

    assert(safe_year <= MAX_SAFE_YEAR && safe_year >= MIN_SAFE_YEAR);

    return safe_year;
}


void pymongo_copy_tm_to_TM64(const struct tm *src, struct TM *dest) {
    if( src == NULL ) {
        memset(dest, 0, sizeof(*dest));
    }
    else {
#       ifdef USE_TM64
            dest->tm_sec        = src->tm_sec;
            dest->tm_min        = src->tm_min;
            dest->tm_hour       = src->tm_hour;
            dest->tm_mday       = src->tm_mday;
            dest->tm_mon        = src->tm_mon;
            dest->tm_year       = (Year)src->tm_year;
            dest->tm_wday       = src->tm_wday;
            dest->tm_yday       = src->tm_yday;
            dest->tm_isdst      = src->tm_isdst;

#           ifdef HAS_TM_TM_GMTOFF
                dest->tm_gmtoff  = src->tm_gmtoff;
#           endif

#           ifdef HAS_TM_TM_ZONE
                dest->tm_zone  = src->tm_zone;
#           endif

#       else
            /* They're the same type */
            memcpy(dest, src, sizeof(*dest));
#       endif
    }
}


void cbson_copy_TM64_to_tm(const struct TM *src, struct tm *dest) {
    if( src == NULL ) {
        memset(dest, 0, sizeof(*dest));
    }
    else {
#       ifdef USE_TM64
            dest->tm_sec        = src->tm_sec;
            dest->tm_min        = src->tm_min;
            dest->tm_hour       = src->tm_hour;
            dest->tm_mday       = src->tm_mday;
            dest->tm_mon        = src->tm_mon;
            dest->tm_year       = (int)src->tm_year;
            dest->tm_wday       = src->tm_wday;
            dest->tm_yday       = src->tm_yday;
            dest->tm_isdst      = src->tm_isdst;

#           ifdef HAS_TM_TM_GMTOFF
                dest->tm_gmtoff  = src->tm_gmtoff;
#           endif

#           ifdef HAS_TM_TM_ZONE
                dest->tm_zone  = src->tm_zone;
#           endif

#       else
            /* They're the same type */
            memcpy(dest, src, sizeof(*dest));
#       endif
    }
}


/* Simulate localtime_r() to the best of our ability */
struct tm * cbson_fake_localtime_r(const time_t *time, struct tm *result) {
    const struct tm *static_result = localtime(time);

    assert(result != NULL);

    if( static_result == NULL ) {
        memset(result, 0, sizeof(*result));
        return NULL;
    }
    else {
        memcpy(result, static_result, sizeof(*result));
        return result;
    }
}


/* Simulate gmtime_r() to the best of our ability */
struct tm * cbson_fake_gmtime_r(const time_t *time, struct tm *result) {
    const struct tm *static_result = gmtime(time);

    assert(result != NULL);

    if( static_result == NULL ) {
        memset(result, 0, sizeof(*result));
        return NULL;
    }
    else {
        memcpy(result, static_result, sizeof(*result));
        return result;
    }
}


static Time64_T seconds_between_years(Year left_year, Year right_year) {
    int increment = (left_year > right_year) ? 1 : -1;
    Time64_T seconds = 0;
    int cycles;

    if( left_year > 2400 ) {
        cycles = (int)((left_year - 2400) / 400);
        left_year -= cycles * 400;
        seconds   += cycles * seconds_in_gregorian_cycle;
    }
    else if( left_year < 1600 ) {
        cycles = (int)((left_year - 1600) / 400);
        left_year += cycles * 400;
        seconds   += cycles * seconds_in_gregorian_cycle;
    }

    while( left_year != right_year ) {
        seconds += length_of_year[IS_LEAP(right_year - 1900)] * 60 * 60 * 24;
        right_year += increment;
    }

    return seconds * increment;
}


Time64_T cbson_mktime64(const struct TM *input_date) {
    struct tm safe_date;
    struct TM date;
    Time64_T  time;
    Year      year = input_date->tm_year + 1900;

    if( cbson_date_in_safe_range(input_date, &SYSTEM_MKTIME_MIN, &SYSTEM_MKTIME_MAX) )
    {
        cbson_copy_TM64_to_tm(input_date, &safe_date);
        return (Time64_T)mktime(&safe_date);
    }

    /* Have to make the year safe in date else it won't fit in safe_date */
    date = *input_date;
    date.tm_year = safe_year(year) - 1900;
    cbson_copy_TM64_to_tm(&date, &safe_date);

    time = (Time64_T)mktime(&safe_date);

    time += seconds_between_years(year, (Year)(safe_date.tm_year + 1900));

    return time;
}


/* Because I think mktime() is a crappy name */
Time64_T timelocal64(const struct TM *date) {
    return cbson_mktime64(date);
}


struct TM *cbson_gmtime64_r (const Time64_T *in_time, struct TM *p)
{
    int v_tm_sec, v_tm_min, v_tm_hour, v_tm_mon, v_tm_wday;
    Time64_T v_tm_tday;
    int leap;
    Time64_T m;
    Time64_T time = *in_time;
    Year year = 70;
    int cycles = 0;

    assert(p != NULL);

#ifdef USE_SYSTEM_GMTIME
    /* Use the system gmtime() if time_t is small enough */
    if( SHOULD_USE_SYSTEM_GMTIME(*in_time) ) {
        time_t safe_time = (time_t)*in_time;
        struct tm safe_date;
        GMTIME_R(&safe_time, &safe_date);

        pymongo_copy_tm_to_TM64(&safe_date, p);
        assert(check_tm(p));

        return p;
    }
#endif

#ifdef HAS_TM_TM_GMTOFF
    p->tm_gmtoff = 0;
#endif
    p->tm_isdst  = 0;

#ifdef HAS_TM_TM_ZONE
    p->tm_zone   = "UTC";
#endif

    v_tm_sec =  (int)(time % 60);
    time /= 60;
    v_tm_min =  (int)(time % 60);
    time /= 60;
    v_tm_hour = (int)(time % 24);
    time /= 24;
    v_tm_tday = time;

    _TIME64_WRAP (v_tm_sec, v_tm_min, 60);
    _TIME64_WRAP (v_tm_min, v_tm_hour, 60);
    _TIME64_WRAP (v_tm_hour, v_tm_tday, 24);

    v_tm_wday = (int)((v_tm_tday + 4) % 7);
    if (v_tm_wday < 0)
        v_tm_wday += 7;
    m = v_tm_tday;

    if (m >= CHEAT_DAYS) {
        year = CHEAT_YEARS;
        m -= CHEAT_DAYS;
    }

    if (m >= 0) {
        /* Gregorian cycles, this is huge optimization for distant times */
        cycles = (int)(m / (Time64_T) days_in_gregorian_cycle);
        if( cycles ) {
            m -= (cycles * (Time64_T) days_in_gregorian_cycle);
            year += (cycles * years_in_gregorian_cycle);
        }

        /* Years */
        leap = IS_LEAP (year);
        while (m >= (Time64_T) length_of_year[leap]) {
            m -= (Time64_T) length_of_year[leap];
            year++;
            leap = IS_LEAP (year);
        }

        /* Months */
        v_tm_mon = 0;
        while (m >= (Time64_T) days_in_month[leap][v_tm_mon]) {
            m -= (Time64_T) days_in_month[leap][v_tm_mon];
            v_tm_mon++;
        }
    } else {
        year--;

        /* Gregorian cycles */
        cycles = (int)((m / (Time64_T) days_in_gregorian_cycle) + 1);
        if( cycles ) {
            m -= (cycles * (Time64_T) days_in_gregorian_cycle);
            year += (cycles * years_in_gregorian_cycle);
        }

        /* Years */
        leap = IS_LEAP (year);
        while (m < (Time64_T) -length_of_year[leap]) {
            m += (Time64_T) length_of_year[leap];
            year--;
            leap = IS_LEAP (year);
        }

        /* Months */
        v_tm_mon = 11;
        while (m < (Time64_T) -days_in_month[leap][v_tm_mon]) {
            m += (Time64_T) days_in_month[leap][v_tm_mon];
            v_tm_mon--;
        }
        m += (Time64_T) days_in_month[leap][v_tm_mon];
    }

    p->tm_year = (int)year;
    if( p->tm_year != year ) {
#ifdef EOVERFLOW
        errno = EOVERFLOW;
#endif
        return NULL;
    }

    /* At this point m is less than a year so casting to an int is safe */
    p->tm_mday = (int) m + 1;
    p->tm_yday = julian_days_by_month[leap][v_tm_mon] + (int)m;
    p->tm_sec  = v_tm_sec;
    p->tm_min  = v_tm_min;
    p->tm_hour = v_tm_hour;
    p->tm_mon  = v_tm_mon;
    p->tm_wday = v_tm_wday;

    assert(check_tm(p));

    return p;
}


struct TM *cbson_localtime64_r (const Time64_T *time, struct TM *local_tm)
{
    time_t safe_time;
    struct tm safe_date;
    struct TM gm_tm;
    Year orig_year;
    int month_diff;

    assert(local_tm != NULL);

#ifdef USE_SYSTEM_LOCALTIME
    /* Use the system localtime() if time_t is small enough */
    if( SHOULD_USE_SYSTEM_LOCALTIME(*time) ) {
        safe_time = (time_t)*time;

        TIME64_TRACE1("Using system localtime for %lld\n", *time);

        LOCALTIME_R(&safe_time, &safe_date);

        pymongo_copy_tm_to_TM64(&safe_date, local_tm);
        assert(check_tm(local_tm));

        return local_tm;
    }
#endif

    if( cbson_gmtime64_r(time, &gm_tm) == NULL ) {
        TIME64_TRACE1("cbson_gmtime64_r returned null for %lld\n", *time);
        return NULL;
    }

    orig_year = gm_tm.tm_year;

    if (gm_tm.tm_year > (2037 - 1900) ||
        gm_tm.tm_year < (1970 - 1900)
       )
    {
        TIME64_TRACE1("Mapping tm_year %lld to safe_year\n", (Year)gm_tm.tm_year);
        gm_tm.tm_year = safe_year((Year)(gm_tm.tm_year + 1900)) - 1900;
    }

    safe_time = (time_t)cbson_timegm64(&gm_tm);
    if( LOCALTIME_R(&safe_time, &safe_date) == NULL ) {
        TIME64_TRACE1("localtime_r(%d) returned NULL\n", (int)safe_time);
        return NULL;
    }

    pymongo_copy_tm_to_TM64(&safe_date, local_tm);

    local_tm->tm_year = (int)orig_year;
    if( local_tm->tm_year != orig_year ) {
        TIME64_TRACE2("tm_year overflow: tm_year %lld, orig_year %lld\n",
              (Year)local_tm->tm_year, (Year)orig_year);

#ifdef EOVERFLOW
        errno = EOVERFLOW;
#endif
        return NULL;
    }


    month_diff = local_tm->tm_mon - gm_tm.tm_mon;

    /*  When localtime is Dec 31st previous year and
        gmtime is Jan 1st next year.
    */
    if( month_diff == 11 ) {
        local_tm->tm_year--;
    }

    /*  When localtime is Jan 1st, next year and
        gmtime is Dec 31st, previous year.
    */
    if( month_diff == -11 ) {
        local_tm->tm_year++;
    }

    /* GMT is Jan 1st, xx01 year, but localtime is still Dec 31st
       in a non-leap xx00.  There is one point in the cycle
       we can't account for which the safe xx00 year is a leap
       year.  So we need to correct for Dec 31st coming out as
       the 366th day of the year.
    */
    if( !IS_LEAP(local_tm->tm_year) && local_tm->tm_yday == 365 )
        local_tm->tm_yday--;

    assert(check_tm(local_tm));

    return local_tm;
}


int cbson_valid_tm_wday( const struct TM* date ) {
    if( 0 <= date->tm_wday && date->tm_wday <= 6 )
        return 1;
    else
        return 0;
}

int cbson_valid_tm_mon( const struct TM* date ) {
    if( 0 <= date->tm_mon && date->tm_mon <= 11 )
        return 1;
    else
        return 0;
}


/* Non-thread safe versions of the above */
struct TM *cbson_localtime64(const Time64_T *time) {
#ifdef _MSC_VER
    _tzset();
#else
    tzset();
#endif
    return cbson_localtime64_r(time, &Static_Return_Date);
}

struct TM *cbson_gmtime64(const Time64_T *time) {
    return cbson_gmtime64_r(time, &Static_Return_Date);
}
