#include <time.h>
#include <sys/time.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "time_stats.h"


void time_stats_init (time_stats_t *ts)
{
    memset (&ts->last_start, 0, sizeof (struct timeval));
    ts->name = 0;
    ts->n = 0;
    ts->head = 0;
    ts->stats_done_at_n = 0;
    ts->tail_latency_enabled = 0;

    memset (&ts->last_start, 0, sizeof (struct timeval));
}

void time_stats_init_named (time_stats_t* ts, char* name)
{
    time_stats_init (ts);

    //maloc a name and copy
    ts->name = malloc (strlen (name)+1);
    strcpy (ts->name, name);    
}

void time_stats_destroy (time_stats_t* ts)
{
    if (ts->name)
        free (ts->name);

    struct time_stats_node *node = ts->head;
    while (ts->head) {
        node = node->next;
        free (ts->head);
        ts->head = node;
    }
    
}

void time_stats_enable_tail_latencies (time_stats_t* ts)
{
    ts->tail_latency_enabled = 1;
}

inline void time_stats_start(time_stats_t *ts)
{
    gettimeofday(&ts->last_start, NULL);
}

inline double time_stats_stop(time_stats_t *ts)
{
    struct timeval end;
    gettimeofday(&end, NULL);

    //put new node in list's head
    struct time_stats_node *node = malloc (sizeof (struct time_stats_node));
    node->start = ts->last_start;
    node->stop  = end;
    node->next  = ts->head;
    ts->head    = node;
    ts->n++;

    struct timeval t_elap;
    timersub(&end, &ts->last_start, &t_elap);
    node->elapsed = (double)(t_elap.tv_sec * 1000000.0 + (double)t_elap.tv_usec) / 1000000.0;
    return node->elapsed;
}

inline double time_stats_stop_dummy(time_stats_t *ts, double in)
{
    //put new node in list's head
    struct time_stats_node *node = malloc (sizeof (struct time_stats_node));
    //node->start = ts->last_start;
    //node->stop  = end;
    node->next  = ts->head;
    ts->head    = node;
    ts->n++;

    node->elapsed = in;
}

/*
 *  Output functions
 */

void time_stats_print (time_stats_t* ts)
{
    time_stats_write (STDOUT_FILENO, ts);
}

void time_stats_sprint (char* str, time_stats_t* ts)
{
    _time_stats_update_stats(ts);

    char* ap = str;

    if (!ts->name) 
        ap += sprintf(ap, "\nTime stats:\n");
    else
        ap += sprintf(ap, "\n\n%s,", ts->name);

    struct time_stats_stats* stats = &ts->stats;

    ap += sprintf(ap, "%.6f,%.6f,%.6f,", stats->avg, stats->min, stats->max);
    ap += sprintf(ap, "%.6f,%.6f", stats->var, stats->std_dev);

    if (ts->tail_latency_enabled) {
        ap += sprintf(ap, "99p\t\t99.9p\t\t99.99p\t\t99.999p\n");
        ap += sprintf(ap, "%.8f\t%.8f\t%.8f\t%.8f\n", stats->lat_99, 
                        stats->lat_99_9,stats->lat_99_99,stats->lat_99_999);
    }
    ap += sprintf(ap, "\n");
}

void time_stats_write (int fd, time_stats_t* ts)
{
    _time_stats_update_stats(ts);

    if (!ts->name) 
        dprintf(fd, "\nTime stats:\n");
    else
        dprintf(fd, "\nTime stats for %s:\n", ts->name);

    struct time_stats_stats* stats = &ts->stats;

    dprintf(fd, "avg\t\tmin\t\tmax\n");
    dprintf(fd, "%.6f\t%.6f\t%.6f\n", stats->avg, stats->min, stats->max);
    dprintf(fd, " . . . \t . . . \t . . .\n");
    
    dprintf(fd, "var\t\tstd-dev\n");
    dprintf(fd, "%.6f\t%.6f\n", stats->var, stats->std_dev);
    dprintf(fd, " . . . \t . . . \t . . .\n");

    if (ts->tail_latency_enabled) {
        dprintf(fd, "99p\t\t99.9p\t\t99.99p\t\t99.999p\n");
        dprintf(fd, "%.8f\t%.8f\t%.8f\t%.8f\n", stats->lat_99, 
                        stats->lat_99_9,stats->lat_99_99,stats->lat_99_999);
    }
    dprintf(fd, "\n\n");
}


/*
 *  Single stat getters, making sure stats are up to date
 */

double time_stats_get_avg   (time_stats_t* ts) {
    _time_stats_update_stats(ts);
    return ts->stats.avg;
}

double time_stats_get_min   (time_stats_t* ts) {
    _time_stats_update_stats(ts);
    return ts->stats.min;
}

double time_stats_get_max   (time_stats_t* ts) {
    _time_stats_update_stats(ts);
    return ts->stats.max;
}

double time_stats_get_variance  (time_stats_t* ts) {
    _time_stats_update_stats(ts);
    return ts->stats.var;
}

double time_stats_get_std_deviation (time_stats_t* ts) {
    _time_stats_update_stats(ts);
    return ts->stats.std_dev;
}

/*
 *
 *  More internal-y functions
 *
 */

void _time_stats_update_stats (time_stats_t *ts)
{
    if (ts->stats_done_at_n != ts->n)
        _time_stats_calc_stats (ts);
}

void _time_stats_calc_stats (time_stats_t *ts)
{
    //start with the basic, required for next steps
    _time_stats_calc_avg_min_max (ts);
    //var and std are ezpz now
    _time_stats_calc_var_std (ts);

    //tail latencies, if enabled
    if (ts->tail_latency_enabled)
        _time_stats_calc_tail_lat (ts);
}

void _time_stats_calc_avg_min_max (time_stats_t *ts)
{
    struct time_stats_node *node;
    double sum = 0.0;
    struct time_stats_stats* stats = &ts->stats;
    stats->min = stats->max = 0.0;

    //iterate all recorded stats
    for (node = ts->head; node ; node = node->next) {
        if (node->elapsed > stats->max)
            stats->max = node->elapsed;

        if (node->elapsed < stats->min || stats->min == 0.0)
            stats->min = node->elapsed;

        sum += node->elapsed;
    }
    stats->avg = sum / ts->n;
}

void _time_stats_calc_var_std (time_stats_t *ts)
{
    struct time_stats_node *node;
    double sum = 0.0;
    double avg = ts->stats.avg;

    for (node = ts->head; node ; node = node->next)
        sum += pow((node->elapsed - avg), 2);

    ts->stats.var     = sum / ts->n;
    ts->stats.std_dev = sqrt(ts->stats.var);
}

static int _time_stats_compare_double (const void * a, const void * b)
{
  if ( *(double*)a <  *(double*)b ) return -1;
  if ( *(double*)a == *(double*)b ) return 0;
  if ( *(double*)a >  *(double*)b ) return 1;
}

void _time_stats_calc_tail_lat (time_stats_t *ts)
{
    struct time_stats_node  *node;
    struct time_stats_stats *stats = &ts->stats;

    uint16_t i = 0;
    double *elapseds = malloc (ts->n * sizeof (double));

    //transform into array for sorting
    for (node = ts->head ; node ; node = node->next)
        elapseds[i++] = node->elapsed;

    //sort
    qsort(elapseds, ts->n, sizeof(double), _time_stats_compare_double);

    //index of tail latencies
    uint16_t _99     = (uint16_t)((float)ts->n * 0.99);
    uint16_t _99_9   = (uint16_t)((float)ts->n * 0.999);
    uint16_t _99_99  = (uint16_t)((float)ts->n * 0.9999);
    uint16_t _99_999 = (uint16_t)((float)ts->n * 0.99999);

    stats->lat_99     = elapseds[_99];
    stats->lat_99_9   = elapseds[_99_9];
    stats->lat_99_99  = elapseds[_99_99];
    stats->lat_99_999 = elapseds[_99_999];

    free (elapseds);
}
