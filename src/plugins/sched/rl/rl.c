/* 
 * Implementaiton of RLScheduler plugin
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/node_select.h"
#include "src/common/parse_time.h"
#include "src/common/read_config.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "src/slurmctld/burst_buffer.h"
#include "src/slurmctld/fed_mgr.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/node_scheduler.h"
#include "src/slurmctld/preempt.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/srun_comm.h"
#include "rl.h"
#include "../../../slurmctld/job_scheduler.h"
#include "../../../slurmctld/slurmctld.h"

#ifndef RL_INTERVAL
#  define RL_INTERVAL   2
#endif

#ifndef DEFAULT_RL_PARAMS
#  define DEFAULT_RL_PARAMS       "n:c:m:t"
#endif

/*********************** local variables *********************/
static bool stop_rl = false;
static pthread_mutex_t term_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  term_cond = PTHREAD_COND_INITIALIZER;
static bool config_flag = false;
static int rl_interval = RL_INTERVAL;
char *rl_params = DEFAULT_RL_PARAMS;
static int max_sched_job_cnt = 1;
static int sched_timeout = 0;

/*********************** local functions *********************/
static void _begin_scheduling(void);
static void _load_config(void);
static void _my_sleep(int secs);

/* Terminate rl_agent */
extern void stop_rl_agent(void)
{
	slurm_mutex_lock(&term_lock);
	stop_rl = true;
	slurm_cond_signal(&term_cond);
	slurm_mutex_unlock(&term_lock);
}

static void _my_sleep(int secs)
{
	struct timespec ts = {0, 0};
	struct timeval now;

	gettimeofday(&now, NULL);
	ts.tv_sec = now.tv_sec + secs;
	ts.tv_nsec = now.tv_usec * 1000;
	slurm_mutex_lock(&term_lock);
	if (!stop_rl)
		slurm_cond_timedwait(&term_cond, &term_lock, &ts);
	slurm_mutex_unlock(&term_lock);
}

static void _load_config(void)
{
	char *sched_params, *select_type, *tmp_ptr;

	sched_timeout = slurm_get_msg_timeout() / 2;
	sched_timeout = MAX(sched_timeout, 1);
	sched_timeout = MIN(sched_timeout, 10);

	sched_params = slurm_get_sched_params();

	if (sched_params && (tmp_ptr=strstr(sched_params, "interval=")))
		rl_interval = atoi(tmp_ptr + 9);
	if (rl_interval < 1) {
		error("Invalid SchedulerParameters interval: %d",
		      rl_interval);
		rl_interval = RL_INTERVAL;
	}

	if (sched_params && (tmp_ptr=strstr(sched_params, "max_job_bf=")))
		max_sched_job_cnt = atoi(tmp_ptr + 11);
	if (sched_params && (tmp_ptr=strstr(sched_params, "bf_max_job_test=")))
		max_sched_job_cnt = atoi(tmp_ptr + 16);
	if (max_sched_job_cnt < 1) {
		error("Invalid SchedulerParameters bf_max_job_test: %d",
		      max_sched_job_cnt);
		max_sched_job_cnt = 50;
	}
	xfree(sched_params);

	select_type = slurm_get_select_type();
	if (!xstrcmp(select_type, "select/serial")) {
		max_sched_job_cnt = 0;
		stop_rl_agent();
	}
	xfree(select_type);

	rl_params = slurm_get_rl_params();
	debug2("RL: loaded rl params: %s", rl_params);
}

static int node_comparator(job_queue_rec_t *job_rec1, job_queue_rec_t *job_rec2)
{
    return (job_rec1->job_ptr->details->min_nodes - job_rec2->job_ptr->details->min_nodes);
}

static int cpu_comparator(job_queue_rec_t *job_rec1, job_queue_rec_t *job_rec2)
{
    return (job_rec1->job_ptr->details->min_cpus - job_rec2->job_ptr->details->min_cpus);
}

static int memory_comparator(job_queue_rec_t *job_rec1, job_queue_rec_t *job_rec2)
{
    return (job_rec1->job_ptr->details->pn_min_memory - job_rec2->job_ptr->details->pn_min_memory);
}

static int submit_time_comparator(job_queue_rec_t *job_rec1, job_queue_rec_t *job_rec2)
{
    return SLURM_DIFFTIME(job_rec1->job_ptr->details->submit_time, job_rec2->job_ptr->details->submit_time);
}

static int composite_comparator(void *x, void *y)
{
    job_queue_rec_t *job_rec1 = *(job_queue_rec_t **) x;
    job_queue_rec_t *job_rec2 = *(job_queue_rec_t **) y;

    int result = 0;

    char* temp_rl_params = xmalloc(sizeof(*rl_params));
    strcpy(temp_rl_params, rl_params);

    char *token = strtok(temp_rl_params, ":");

    while (token != NULL)
    {
        switch (token[0])
        {
            case (int) 'n':
                result = node_comparator(job_rec1, job_rec2);
                break;
            case (int) 'c':
                result = cpu_comparator(job_rec1, job_rec2);
                break;
            case (int) 'm':
                result = memory_comparator(job_rec1, job_rec2);
                break;
            case (int) 't':
                result = submit_time_comparator(job_rec1, job_rec2);
                break;
            default:
                result = -1;
        }

        token = strtok(NULL, ":");

        if (result != 0)
            break;
    }

    if (result == 0)
        result = submit_time_comparator(job_rec1, job_rec2);

    xfree(temp_rl_params);

    return result;
}

static void _begin_scheduling(void)
{
	int j, rc = SLURM_SUCCESS, job_cnt = 0;
	List job_queue;
	job_queue_rec_t *job_queue_rec;
	struct job_record *job_ptr;
	struct part_record *part_ptr;
	bitstr_t *alloc_bitmap = NULL, *avail_bitmap = NULL;
	bitstr_t *exc_core_bitmap = NULL;
	uint32_t max_nodes, min_nodes;
	time_t now = time(NULL), sched_start;
	bool resv_overlap = false;
	sched_start = now;
	alloc_bitmap = bit_alloc(node_record_count);
	job_queue = build_job_queue(true, false);

	/**
	 * Sort the list based on rl_params configuration
	 */
    debug2("RL: current rl params: %s", rl_params);
	list_sort(job_queue, composite_comparator);

	while ((job_queue_rec = (job_queue_rec_t *) list_pop(job_queue))) {
		job_ptr  = job_queue_rec->job_ptr;
		part_ptr = job_queue_rec->part_ptr;
		xfree(job_queue_rec);
		if (part_ptr != job_ptr->part_ptr)
			continue;	/* Only test one partition */

		if (++job_cnt > max_sched_job_cnt) {
			debug2("scheduling loop exiting after %d jobs",
			       max_sched_job_cnt);
			break;
		}

		/* Determine minimum and maximum node counts */
		min_nodes = MAX(job_ptr->details->min_nodes,
				part_ptr->min_nodes);

		if (job_ptr->details->max_nodes == 0)
			max_nodes = part_ptr->max_nodes;
		else
			max_nodes = MIN(job_ptr->details->max_nodes,
					part_ptr->max_nodes);

		max_nodes = MIN(max_nodes, 500000);     /* prevent overflows */

		if (min_nodes > max_nodes) {
			/* job's min_nodes exceeds partition's max_nodes */
			continue;
		}

		j = job_test_resv(job_ptr, &now, true, &avail_bitmap,
				  &exc_core_bitmap, &resv_overlap, false);

		if (j != SLURM_SUCCESS) {
			FREE_NULL_BITMAP(avail_bitmap);
			FREE_NULL_BITMAP(exc_core_bitmap);
			continue;
		}

		// actual resource allocation
        rc = select_nodes(job_ptr, false, NULL, NULL, false);

        if (rc == SLURM_SUCCESS) {
            /* job initiated */
            last_job_update = time(NULL);
            debug2("RL: Started JobId %d on %s",
                 job_ptr->job_id, job_ptr->nodes);
            if (job_ptr->batch_flag == 0)
                srun_allocate(job_ptr);
            else if (!IS_JOB_CONFIGURING(job_ptr))
                launch_job(job_ptr);
        }

		FREE_NULL_BITMAP(avail_bitmap);
		FREE_NULL_BITMAP(exc_core_bitmap);

		if ((time(NULL) - sched_start) >= sched_timeout) {
			debug2("scheduling loop exiting after %d jobs",
			       max_sched_job_cnt);
			break;
		}
	}
	FREE_NULL_LIST(job_queue);
	FREE_NULL_BITMAP(alloc_bitmap);
}

/* Note that slurm.conf has changed */
extern void rl_reconfig(void)
{
	config_flag = true;
}

/* rl_agent - detached thread */
extern void *rl_agent(void *args)
{
	time_t now;
	double wait_time;
	static time_t last_sched_time = 0;
	/* Read config, nodes and partitions; Write jobs */
	slurmctld_lock_t all_locks = {
		READ_LOCK, WRITE_LOCK, READ_LOCK, READ_LOCK, READ_LOCK };

	_load_config();
	last_sched_time = time(NULL);
	while (!stop_rl) {
		_my_sleep(rl_interval);
		if (stop_rl)
			break;
		if (config_flag) {
			config_flag = false;
			_load_config();
		}
		now = time(NULL);
		wait_time = difftime(now, last_sched_time);
		if ((wait_time < rl_interval))
			continue;

		lock_slurmctld(all_locks);
        _begin_scheduling();
		last_sched_time = time(NULL);
		(void) bb_g_job_try_stage_in();
		unlock_slurmctld(all_locks);
	}
	return NULL;
}
