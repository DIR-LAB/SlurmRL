/*****************************************************************************\
 *  proctrack_cray_aries.c - process tracking via Cray's API with Aries.
 *****************************************************************************
 *  Copyright (C) 2013 SchedMD LLC
 *  Written by Danny Auble <da@schedmd.com> who borrowed heavily from
 *  the proctrack/sgi_job plugin
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include "config.h"

#define _GNU_SOURCE

#include <dlfcn.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef HAVE_NATIVE_CRAY
#include <job.h>	/* Cray's job module component */
#else
#define jid_t int
#endif

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/timers.h"

#include "src/slurmd/common/proctrack.h"
#include "src/slurmd/slurmstepd/slurmstepd_job.h"

/* These are defined here so when we link with something other than
 * the slurmctld we will have these symbols defined.  They will get
 * overwritten when linking with the slurmctld.
 */
#if defined (__APPLE__)
extern slurm_conf_t slurm_conf __attribute__((weak_import));
#else
slurm_conf_t slurm_conf;
#endif

const char plugin_name[]      = "Process tracking via Cray/Aries job module";
const char plugin_type[]      = "proctrack/cray_aries";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

/*
 *  Handle to libjob.so
 */
static void *libjob_handle = NULL;
static pthread_t threadid = 0;
static pthread_cond_t notify = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t notify_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER;

extern bool proctrack_p_has_pid (uint64_t cont_id, pid_t pid);

static void *_create_container_thread(void *args)
{
	stepd_step_rec_t *step = (stepd_step_rec_t *)args;
#ifdef HAVE_NATIVE_CRAY
	step->cont_id = (uint64_t)job_create(0, step->uid, 0);
#endif
	/* Signal the container_create we are done */
	slurm_mutex_lock(&notify_mutex);

	/* We need to signal failure or not */
	slurm_cond_signal(&notify);

	/*
	 * Don't unlock the notify_mutex here, wait, it is not needed
	 * and can cause deadlock if done.
	 */

	if (step->cont_id == (jid_t) -1) {
		error("Failed to create job container: %m");
	} else {
		/*
		 * Wait around for something else to be added and then exit
		 * when that takes place.
		 */
		slurm_cond_wait(&notify, &notify_mutex);
	}
	slurm_mutex_unlock(&notify_mutex);

	return NULL;
}

static void _end_container_thread(void)
{
	if (threadid) {
		/* This will end the thread and remove it from the container */
		slurm_mutex_lock(&thread_mutex);
		slurm_mutex_lock(&notify_mutex);
		slurm_cond_signal(&notify);
		slurm_mutex_unlock(&notify_mutex);

		pthread_join(threadid, NULL);
		threadid = 0;
		slurm_mutex_unlock(&thread_mutex);
	}
}

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
extern int init(void)
{
	debug("%s loaded", plugin_name);
	return SLURM_SUCCESS;
}

extern int fini(void)
{
	_end_container_thread();

	/* free up some memory */
	slurm_mutex_destroy(&notify_mutex);
	slurm_cond_destroy(&notify);
	slurm_mutex_destroy(&thread_mutex);

	return SLURM_SUCCESS;
}

extern int proctrack_p_create(stepd_step_rec_t *step)
{
	DEF_TIMERS;
	START_TIMER;

	if (!libjob_handle)
		init();

	if (!step->cont_id) {
		/*
		 * If we are forked then we can just use the pid from the fork
		 * instead of using the thread method below.
		 */
		if (proctrack_forked) {
#ifdef HAVE_NATIVE_CRAY
			step->cont_id = (uint64_t) job_create(0, step->uid, 0);
#endif
			goto endit;
		}

		/*
		 * Since the cray job lib will create the container off the
		 * process calling job_create we don't want to call it from
		 * the main process since it will include all the threads
		 * the main process spawns and there is no way to safely track
		 * which pids need to be removed when removing the parent.
		 * It turns out spawning a thread will make the job_create
		 * create the container off that process instead of the main
		 * process.  Once we have added a process we can end the
		 * thread which will remove the pid from the container
		 * automatically.  Empty containers are not valid.
		 */
		slurm_mutex_lock(&thread_mutex);
		if (threadid) {
			debug("Had a thread already 0x%08lx", threadid);
			slurm_mutex_lock(&notify_mutex);
			slurm_cond_wait(&notify, &notify_mutex);
			slurm_mutex_unlock(&notify_mutex);
			debug("Last thread done 0x%08lx", threadid);
		}

		/*
		 * We have to lock the notify_mutex here since the
		 * thread could possibly signal things before we
		 * started waiting for it.
		 */
		slurm_mutex_lock(&notify_mutex);
		slurm_thread_create(&threadid, _create_container_thread, step);
		slurm_cond_wait(&notify, &notify_mutex);
		slurm_mutex_unlock(&notify_mutex);
		slurm_mutex_unlock(&thread_mutex);
		if (step->cont_id != (jid_t) -1)
			debug("proctrack_p_create: created jid 0x%08"PRIx64" thread 0x%08lx",
			      step->cont_id, threadid);
	} else
		error("proctrack_p_create: already have a cont_id");
endit:
	END_TIMER;
	if (slurm_conf.debug_flags & DEBUG_FLAG_TIME_CRAY)
		INFO_LINE("call took: %s", TIME_STR);

	return SLURM_SUCCESS;
}

/*
 * NOTE: This function is called after slurmstepd spawns all user tasks.
 * Since the slurmstepd was placed in the job container when the container
 * was created and all of it's spawned tasks are placed into the container
 * when forked, all we need to do is remove the slurmstepd from the container
 * (once) at this time.
 */
int proctrack_p_add(stepd_step_rec_t *step, pid_t pid)
{
#ifdef HAVE_NATIVE_CRAY
	char fname[64];
	int fd;
	uint32_t jobid;
	int count = 0;

	DEF_TIMERS;
	START_TIMER;

try_again:
	/*
	 * If we aren't forked (pid was added in the job_create() call) this is
	 * the time to add the pid to the job container.
	 */
	if (!proctrack_forked &&
	    job_attachpid(pid, step->cont_id) == (jid_t) -1) {
		if (errno == EINVAL && (count < 1)) {
			jid_t jid;
			if (proctrack_p_has_pid(step->cont_id, pid)) {
				debug("%s: Trying to add pid (%d) again to the same container, ignoring.",
				      __func__, pid);
				return SLURM_SUCCESS;
			}

			if ((jid = job_detachpid(pid)) != (jid_t) -1) {
				error("%s: Pid %d was attached to container %"PRIu64" incorrectly.  Moving to correct (%"PRIu64").",
				      __func__, pid, jid, step->cont_id);
				count++;
				goto try_again;
			} else {
				error("%s: Couldn't detach pid %d from container: %m",
				      __func__, pid);
				return SLURM_ERROR;
			}
		} else {
			error("Failed to attach pid %d to job container: %m",
			      pid);
			return SLURM_ERROR;
		}
	}
	_end_container_thread();

	// Set apid for this pid
	if (step->het_job_id && (step->het_job_id != NO_VAL))
		jobid = step->het_job_id;
	else
		jobid = step->step_id.job_id;
	if (job_setapid(pid, SLURM_ID_HASH(jobid,
					   step->step_id.step_id)) == -1) {
		error("Failed to set pid %d apid: %m", pid);
		return SLURM_ERROR;
	}

	// Explicitly mark pid as an application (/proc/<pid>/task_is_app)
	snprintf(fname, sizeof(fname), "/proc/%d/task_is_app", pid);
	fd = open(fname, O_WRONLY);
	if (fd == -1) {
		error("Failed to open %s: %m", fname);
		return SLURM_ERROR;
	}
	if (write(fd, "1", 1) < 1) {
		error("Failed to write to %s: %m", fname);
		TEMP_FAILURE_RETRY(close(fd));
		return SLURM_ERROR;
	}
	TEMP_FAILURE_RETRY(close(fd));
	END_TIMER;
	if (slurm_conf.debug_flags & DEBUG_FLAG_TIME_CRAY)
		INFO_LINE("call took: %s", TIME_STR);
#endif

	return SLURM_SUCCESS;
}

int proctrack_p_signal(uint64_t id, int sig)
{
#ifdef HAVE_NATIVE_CRAY
	DEF_TIMERS;
	START_TIMER;
	if (!threadid) {
		if ((job_killjid((jid_t) id, sig) < 0)
		    && (errno != ENODATA) && (errno != EBADF) )
			return (SLURM_ERROR);
	} else if (sig == SIGKILL) {
		/* job ended before it started */
		_end_container_thread();
	} else
		error("Trying to send signal %d a container 0x%08lx "
		      "that hasn't had anything added to it yet", sig, id);
	END_TIMER;
	if (slurm_conf.debug_flags & DEBUG_FLAG_TIME_CRAY)
		INFO_LINE("call took: %s", TIME_STR);
#endif
	return (SLURM_SUCCESS);
}

int proctrack_p_destroy(uint64_t id)
{
#ifdef HAVE_NATIVE_CRAY
	int status;
	DEF_TIMERS;
	START_TIMER;

	debug("destroying 0x%08lx 0x%08lx", id, threadid);

	if (!threadid)
		job_waitjid((jid_t) id, &status, 0);

	/*
	 * Assume any error means job doesn't exist. Therefore,
	 * return SUCCESS to slurmd so it doesn't retry continuously
	 */
	END_TIMER;
	if (slurm_conf.debug_flags & DEBUG_FLAG_TIME_CRAY)
		INFO_LINE("call took: %s", TIME_STR);
#endif
	return SLURM_SUCCESS;
}

uint64_t proctrack_p_find(pid_t pid)
{
	jid_t jid = 0;
#ifdef HAVE_NATIVE_CRAY
	DEF_TIMERS;
	START_TIMER;

	if ((jid = job_getjid(pid)) == (jid_t) -1)
		return ((uint64_t) 0);
	END_TIMER;
	if (slurm_conf.debug_flags & DEBUG_FLAG_TIME_CRAY)
		INFO_LINE("call took: %s", TIME_STR);

#endif
	return ((uint64_t) jid);
}

bool proctrack_p_has_pid (uint64_t cont_id, pid_t pid)
{
#ifdef HAVE_NATIVE_CRAY
	jid_t jid;

	if ((jid = job_getjid(pid)) == (jid_t) -1)
		return false;
	if ((uint64_t)jid != cont_id)
		return false;
#endif
	return true;
}

int proctrack_p_wait(uint64_t id)
{
#ifdef HAVE_NATIVE_CRAY
	int status;

	if (!threadid && job_waitjid((jid_t) id, &status, 0) == (jid_t)-1)
		return SLURM_ERROR;

#endif
	return SLURM_SUCCESS;
}

int proctrack_p_get_pids(uint64_t cont_id, pid_t **pids, int *npids)
{
#ifdef HAVE_NATIVE_CRAY
	int pidcnt, bufsize;
	pid_t *p;
	DEF_TIMERS;
	START_TIMER;

	pidcnt = job_getpidcnt((jid_t)cont_id);
	if (pidcnt > 0) {
		/*
		 * FIXME - The "+ 128" is a rough attempt to allow for
		 * the fact that _job_getpidcnt() followed by _job_get_pidlist
		 * is not atomic.
		 */
		bufsize = sizeof(pid_t) * (pidcnt + 128);
		p = (pid_t *)xmalloc(bufsize);
		pidcnt = job_getpidlist((jid_t)cont_id, p, bufsize);
		if (pidcnt == -1) {
			int rc = SLURM_SUCCESS;
			/*
			 * There is a possiblity for a race condition
			 * where if the last task in the job exits
			 * between job_getpidcnt and job_getpidlist.
			 * That is ok, so just return SUCCESS;
			 */
			if (errno != ENODATA) {
				rc = SLURM_ERROR;
				error("job_getpidlist() failed: %m");
			}

			*pids = NULL;
			*npids = 0;
			xfree(p);

			return rc;
		}
		*pids = p;
		*npids = pidcnt;
	} else {
		*pids = NULL;
		*npids = 0;
	}
	END_TIMER;
	if (slurm_conf.debug_flags & DEBUG_FLAG_TIME_CRAY)
		INFO_LINE("call took: %s", TIME_STR);

#endif
	return SLURM_SUCCESS;
}
