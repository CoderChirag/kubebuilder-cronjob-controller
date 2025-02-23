/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	kbatchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "coderchirag.github.io/cronjob/api/v1"
)

//Clock
type realClock struct {}

func(realClock) Now() time.Time {
	return time.Now()
}

// Clock knows how to get the current time.
// It can be used to fake out timing for testing.
type Clock interface {
	Now() time.Time
}

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

// +kubebuilder:rbac:groups=batch.coderchirag.github.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.coderchirag.github.io,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.coderchirag.github.io,resources=cronjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

var (
	scheduledTimeAnnotation = "batch.coderchirag.github.io/scheduled-at"
)

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.20.0/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var cronJob batchv1.CronJob
	var childJobs kbatchv1.JobList
	var activeJobs, failedJobs, successfulJobs []*kbatchv1.Job
	var mostRecentTime *time.Time
	
	// 1. Load the CronJob by name
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "unable to fetch CronJob")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. List all active jobs, and update the status
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list child Jobs")
		return ctrl.Result{}, err
	}

	mostRecentTime = categorizeJobsAndGetMostRecentStartTime(&log, &childJobs, &activeJobs, &failedJobs, &successfulJobs)
	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))
	if err := r.updateCronjobStatus(ctx, &log, &cronJob, mostRecentTime, activeJobs); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Clean up old jobs according to the history limit
	r.cleanupJobs(ctx, &log, &cronJob, failedJobs, successfulJobs)

	// 4. Check if we're suspended
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// 5. Get the next scheduled run
	// figure out the next times that we need to create
	// jobs at (or anything we missed).
	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out CronJob schedule")
		// we don't really care about requeuing until we get an update that
		// fixes the schedule, so don't return an error
		return ctrl.Result{}, nil
	}
	
	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())} // save this so we can re-use it elsewhere
	log = log.WithValues("now", r.Now(), "next run", nextRun)

	// 6. Run a new job if it's on schedule, not past the deadline, and not blocked by our concurrency policy
	if missedRun.IsZero() {
		log.V(1).Info("no upcoming scheduled time, sleeping until next")
		return scheduledResult, nil
	}

	// make sure we're not too late to start the run
	log = log.WithValues("current run", missedRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Now())
	}
	if tooLate {
		log.V(1).Info("missed starting deadline for last run, sleeping till next")
		return scheduledResult, nil
	}

	// figure out how to run this job -- concurrency policy might forbid us from running
	// multiple at the same time...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
	}

	// ...or instruct us to replace existing ones...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			// we don't care if the job was already deleted
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err
			}
		}
	}

	// actually make the job...
	job, err := r.constructJobFromCronJob(&cronJob, missedRun)
	if err != nil {
		log.Error(err, "unable to construct job from template")
		return ctrl.Result{}, err
	}
	// ...and create it on the cluster
	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create Job for CronJob", "job", job)
		return ctrl.Result{}, err
	}
	log.V(1).Info("created Job for CronJob run", "job", job)

	// we'll requeue once we see the running job, and update our status
	return scheduledResult, nil
}

var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr = batchv1.GroupVersion.String()
)

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// set up a real clock, since we're not in a test
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatchv1.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		// grab the job object, extract the owner...
		job := rawObj.(*kbatchv1.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatchv1.Job{}).
		Named("cronjob").
		Complete(r)
}

func (r *CronJobReconciler) updateCronjobStatus(ctx context.Context, log *logr.Logger, cronJob *batchv1.CronJob, mostRecentTime *time.Time, activeJobs []*kbatchv1.Job) error {
	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	}else {
		cronJob.Status.LastScheduleTime = nil
	}

	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	if err := r.Status().Update(ctx, cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return err
	}

	return nil
}

func (r *CronJobReconciler) cleanupJobs(ctx context.Context, log *logr.Logger, cronJob *batchv1.CronJob, failedJobs, successfulJobs []*kbatchv1.Job) {
	sort.Slice(failedJobs, func(i, j int) bool {
		if failedJobs[i].Status.StartTime == nil {
			return failedJobs[j].Status.StartTime != nil
		}
		return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
	})
	sort.Slice(successfulJobs, func(i, j int) bool {
		if successfulJobs[i].Status.StartTime == nil {
			return successfulJobs[j].Status.StartTime != nil
		}
		return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
	})

	// NB: deleting these are "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish the deleting.
	for i, failedJob := range failedJobs {
		if int32(i) >= int32(len(failedJobs))-cronJob.GetFailedJobHistoryLimit() {
			break
		}
		if err := r.Delete(ctx, failedJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to delete old failed job", "job", failedJob)
		}else{
			log.V(0).Info("deleted old failed job", "job", failedJob)
		}
	}
	for i, successfulJob := range successfulJobs {
		if int32(i) >= int32(len(successfulJobs))-cronJob.GetSuccessfulJobHistoryLimit() {
			break
		}
		if err := r.Delete(ctx, successfulJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to delete old successful job", "job", successfulJob)
		}else{
			log.V(0).Info("deleted old successful job", "job", successfulJob)
		}
	}
}

func (r *CronJobReconciler) constructJobFromCronJob(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatchv1.Job, error) {
	  // We want job names for a given nominal start time to have a deterministic name to avoid the same job being created twice
		name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

		job := &kbatchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels: make(map[string]string),
				Annotations: make(map[string]string),
				Name: name,
				Namespace: cronJob.Namespace,
			},
			Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
		}

		for k, v := range cronJob.Spec.JobTemplate.Labels {
			job.Labels[k] = v
		}
		for k, v := range cronJob.Spec.JobTemplate.Annotations {
			job.Annotations[k] = v
		}
		job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)

		if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
			return nil, err
		}

		return job, nil
}