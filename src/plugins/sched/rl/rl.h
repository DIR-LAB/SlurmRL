/*
 *  rl.h - header for RLScheduler scheduler plugin.
 */

#ifndef _SLURM_RL_H
#define _SLURM_RL_H

/* rl_agent - detached thread periodically when pending jobs can start */
extern void *rl_agent(void *args);

/* Terminate rl_agent */
extern void stop_rl_agent(void);

/* Note that slurm.conf has changed */
extern void rl_reconfig(void);

#endif	/* _SLURM_RL_H */
