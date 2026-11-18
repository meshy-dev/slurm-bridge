// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmbridge

import (
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	podv1 "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/SlinkyProject/slurm-bridge/internal/wellknown"
)

type claimDevice struct{ driver, pool, device string }

func (d claimDevice) String() string {
	return d.driver + "/" + d.pool + "/" + d.device
}

// deviceHeldError reports that a live claim still holds a device that Slurm
// has already assigned to the claim being bound.
type deviceHeldError struct {
	device claimDevice
	holder *resourcev1.ResourceClaim
}

func (e *deviceHeldError) Error() string {
	return fmt.Sprintf("DRA device %s is still allocated to claim %s", e.device, claimOwner(e.holder))
}

func claimOwner(claim *resourcev1.ResourceClaim) string {
	return claim.Namespace + "/" + claim.Name + "/" + string(claim.UID)
}

func claimedDevices(claim *resourcev1.ResourceClaim) map[claimDevice]bool {
	devices := make(map[claimDevice]bool)
	if claim.Status.Allocation == nil {
		return devices
	}
	for _, result := range claim.Status.Allocation.Devices.Results {
		if ptr.Deref(result.AdminAccess, false) {
			continue
		}
		id := claimDevice{result.Driver, result.Pool, result.Device}
		shared := result.ShareID != nil
		if previous, found := devices[id]; found {
			shared = shared && previous
		}
		devices[id] = shared
	}
	return devices
}

// heldDevice returns a device in devices that other also holds, unless both
// allocations share it.
func heldDevice(devices map[claimDevice]bool, other *resourcev1.ResourceClaim) (claimDevice, bool) {
	for id, otherShared := range claimedDevices(other) {
		if shared, found := devices[id]; found && (!shared || !otherShared) {
			return id, true
		}
	}
	return claimDevice{}, false
}

// patchClaimAllocation serializes the live ownership check and conditional write.
// ResourceClaim reads must use the direct API client, as they do in New.
// Only ambiguous failures survive this call; successful writes need no local cache.
func (sb *SlurmBridge) patchClaimAllocation(ctx context.Context, claim, toUpdate *resourcev1.ResourceClaim) error {
	sb.claimMu.Lock()
	defer sb.claimMu.Unlock()
	claims := &resourcev1.ResourceClaimList{}
	if err := sb.List(ctx, claims); err != nil {
		return err
	}
	for owner, uncertain := range sb.uncertainClaims {
		// An absent/replaced claim or changed version fences the old conditional
		// PATCH: it either already committed or can no longer commit.
		if !slices.ContainsFunc(claims.Items, func(current resourcev1.ResourceClaim) bool {
			return claimOwner(&current) == owner && current.ResourceVersion == uncertain.ResourceVersion
		}) {
			delete(sb.uncertainClaims, owner)
		}
	}
	owner, devices := claimOwner(claim), claimedDevices(toUpdate)
	for other, uncertain := range sb.uncertainClaims {
		if other == owner {
			if !maps.Equal(devices, claimedDevices(uncertain)) {
				return fmt.Errorf("claim %s has an unresolved allocation for different devices", owner)
			}
		} else if device, held := heldDevice(devices, uncertain); held {
			return fmt.Errorf("DRA device %s has an unresolved allocation to claim %s", device, other)
		}
	}
	for i := range claims.Items {
		other := &claims.Items[i]
		if claimOwner(other) == owner {
			continue
		}
		if device, held := heldDevice(devices, other); held {
			return &deviceHeldError{device: device, holder: other}
		}
	}
	err := sb.Status().Patch(ctx, toUpdate, client.StrategicMergeFrom(claim, client.MergeFromWithOptimisticLock{}))
	if err == nil {
		delete(sb.uncertainClaims, owner)
	} else if !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) && !apierrors.IsInvalid(err) &&
		!apierrors.IsForbidden(err) && !apierrors.IsUnauthorized(err) && !apierrors.IsBadRequest(err) {
		if sb.uncertainClaims == nil {
			sb.uncertainClaims = make(map[string]*resourcev1.ResourceClaim)
		}
		if sb.uncertainClaims[owner] == nil {
			uncertain := toUpdate.DeepCopy()
			uncertain.ResourceVersion = claim.ResourceVersion
			sb.uncertainClaims[owner] = uncertain
		}
	}
	return err
}

// deletePreemptedHolder deletes the bridge pod that holds claim once that
// pod's Slurm job has stopped running, as the pod controller would on its next
// job-cache refresh. Slurm has already given the device to another job, so
// deleting the pod now frees the device for the waiting pod sooner.
func (sb *SlurmBridge) deletePreemptedHolder(ctx context.Context, claim *resourcev1.ResourceClaim) {
	logger := klog.FromContext(ctx)
	for _, ref := range claim.Status.ReservedFor {
		if ref.APIGroup != "" || ref.Resource != "pods" {
			continue
		}
		pod := &corev1.Pod{}
		if err := sb.Get(ctx, client.ObjectKey{Namespace: claim.Namespace, Name: ref.Name}, pod); err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to get pod holding a device", "pod", klog.KRef(claim.Namespace, ref.Name))
			}
			continue
		}
		if pod.UID != ref.UID || pod.DeletionTimestamp != nil || podv1.IsPodTerminal(pod) ||
			pod.Spec.SchedulerName != sb.schedulerName || pod.Labels[wellknown.LabelExternalJobId] == "" {
			continue
		}
		running, err := sb.slurmControl.IsJobRunning(ctx, pod)
		if err != nil {
			logger.Error(err, "failed to check the Slurm job of a pod holding a device", "pod", klog.KObj(pod))
			continue
		}
		if running {
			continue
		}
		logger.Info("Deleting pod whose Slurm job ended while it still holds a device",
			"pod", klog.KObj(pod), "jobId", pod.Labels[wellknown.LabelExternalJobId], "claim", klog.KObj(claim))
		if err := sb.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete pod holding a device", "pod", klog.KObj(pod))
		}
	}
}
