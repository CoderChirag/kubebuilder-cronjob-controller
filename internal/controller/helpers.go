package controller

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron"
	kbatchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	batchv1 "coderchirag.github.io/cronjob/api/v1"
)

func isJobFinished(job *kbatchv1.Job) (bool, kbatchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == kbatchv1.JobComplete || c.Type == kbatchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

func getScheduledTimeForJob(job *kbatchv1.Job) (*time.Time, error) {
	timeRaw := job.Annotations[scheduledTimeAnnotation]
	if len(timeRaw) == 0 {
		return nil, nil
	}

	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return nil, err
	}
	return &timeParsed, nil
}

func categorizeJobsAndGetMostRecentStartTime(log *logr.Logger, jobs *kbatchv1.JobList, activeJobs, failedJobs, successfulJobs *[]*kbatchv1.Job) *time.Time {
	var mostRecentTime *time.Time
	for i, job := range jobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": //active job
			*activeJobs = append(*activeJobs, &jobs.Items[i])
		case kbatchv1.JobFailed:
			*failedJobs = append(*failedJobs, &jobs.Items[i])
		case kbatchv1.JobComplete:
			*successfulJobs = append(*successfulJobs, &jobs.Items[i])
		}
		// We'll store the launch time in an annotation, so we'll reconstitute that from
		// the active jobs themselves.
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		if mostRecentTime == nil || mostRecentTime.Before(*scheduledTimeForJob) {
			mostRecentTime = scheduledTimeForJob
		}
	}
	return mostRecentTime
}

func getNextSchedule(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
	sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("unparseable schedule %q: %v", cronJob.Spec.Schedule, err)
	}
	// for optimization purposes, cheat a bit and start from our last observed run time
	// we could reconstitute this here, but there's not much point, since we've
	// just updated it.
	var earliestTime time.Time
	if cronJob.Status.LastScheduleTime != nil {
		earliestTime = cronJob.Status.LastScheduleTime.Time
	} else {
		earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
	}

	if cronJob.Spec.StartingDeadlineSeconds != nil {
		// controller is not going to schedule anything below this point
		schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))

		if schedulingDeadline.After(earliestTime) {
			// we don't have to consider any time before the deadline, and thus setting earliest time to deadline
			earliestTime = schedulingDeadline
		}
	}

	if earliestTime.After(now) {
		// This should never happen in normal cases, adding this just to handle any clock skews by cluster.
		return time.Time{}, sched.Next(now), nil
	}

	starts := 0
	for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
		lastMissed = t
		// An object might miss several starts. For example, if
		// controller gets wedged on Friday at 5:01pm when everyone has
		// gone home, and someone comes in on Tuesday AM and discovers
		// the problem and restarts the controller, then all the hourly
		// jobs, more than 80 of them for one hourly scheduledJob, should
		// all start running with no further intervention (if the scheduledJob
		// allows concurrency and late starts).

		// However, if there is a bug somewhere, or incorrect clock
		// on controller's server or apiservers (for setting creationTimestamp)
		// then there could be so many missed start times (it could be off
		// by decades or more), that it would eat up all the CPU and memory
		// of this controller. In that case, we want to not try to list
		// all the missed start times.
		starts++
		if starts > 100 {
			return time.Time{}, time.Time{}, fmt.Errorf("too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew")
		}
	}
	return lastMissed, sched.Next(now), nil
}
