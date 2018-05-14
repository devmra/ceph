#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 *  A one measurement list node, created every time_stats_stop
 * start and stop are not being used, but being kept if extra information
 * is needed.
 */

 struct time_stats_node
 {
    struct timeval start;
    struct timeval stop;
    double elapsed;
    struct time_stats_node *next;
 };

/*
 *  Statistics for all measurements
 */

 struct time_stats_stats
 {
    double avg;
    double min, max;
    double var, std_dev;
    //tail latencies
    double lat_99, lat_99_9, lat_99_99, lat_99_999;
 };

/*
 * Main struct, holds all information
 */

struct time_stats
{
    struct timeval last_start;
    char* name;
    uint16_t n;
    struct time_stats_node *head;
    struct time_stats_stats stats;
    
    uint8_t tail_latency_enabled; /* flag to enable tail latency, default off*/
    uint16_t stats_done_at_n;  /* at which n the stats were calculated */
};

typedef struct time_stats time_stats_t;


/*
 *  Function prototypes
 */

void time_stats_init       (time_stats_t* ts);
void time_stats_init_named (time_stats_t* ts, char* name);
void time_stats_destroy    (time_stats_t* ts);
void time_stats_enable_tail_latencies (time_stats_t* ts);
//The name will be used for pretty outputting later, eg:
// time_stats_init_named(..,"Float division");
//Then when we print something like this will show up:
// Stats for "Float division"
//Don't forget to destroy, otherwise the list will not be freed


void   time_stats_start (time_stats_t* ts);
double time_stats_stop  (time_stats_t* ts);
double time_stats_stop_dummy(time_stats_t*, double);
//Main functions, start before measuring, stop after
//Stop returns the elapsed seconds for convenience, time is recorded in
//the list anyway


void time_stats_print   (time_stats_t*);
void time_stats_sprint  (char*, time_stats_t*);
void time_stats_write   (int fd, time_stats_t* ts);
//Print outputs to stdout or fd


double time_stats_get_avg   (time_stats_t* ts);
double time_stats_get_min   (time_stats_t* ts);
double time_stats_get_max   (time_stats_t* ts);
double time_stats_get_variance  (time_stats_t* ts);
double time_stats_get_std_deviation (time_stats_t* ts);
//In case only specific stats are required or you want to use them in
//a different way, use these.


/*
 *  The following are somewhat more internal functions to keep stuff
 *  up to date and actually calculate the stats
 */

void _time_stats_update_stats   (time_stats_t *ts);
void _time_stats_calc_stats     (time_stats_t *ts);
void _time_stats_calc_avg_min_max (time_stats_t *ts);
void _time_stats_calc_var_std   (time_stats_t *ts);
void _time_stats_calc_tail_lat  (time_stats_t *ts);

#ifdef __cplusplus
}
#endif
