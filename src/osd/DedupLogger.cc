#include "DedupLogger.h"
#include <stdio.h>
#include <stdarg.h>
#include "common/Mutex.h"

#define DLOG 0

static Mutex loglock("loglock");

void dlog (const char * format, ...)
{
#if DLOG
  Mutex::Locker lock(loglock);
  FILE* file;
  file = fopen ("/home/ubuntu/myceph.log", "a+");

  va_list args;
  va_start (args, format);
  vfprintf (file, format, args);
  va_end (args);

  fclose (file);
#endif
}


void idlog (const char * format, ...)
{
  Mutex::Locker lock(loglock);
  FILE* file;
  file = fopen ("/home/ubuntu/stats_myceph.log", "a+");

  va_list args;
  va_start (args, format);
  vfprintf (file, format, args);
  va_end (args);

  fclose (file);
}
