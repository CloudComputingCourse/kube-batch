/*
Copyright 2018 The Kubernetes Authors.

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

package allocate

import (
	"k8s.io/apimachinery/pkg/labels"

	"github.com/golang/glog"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/scheduler/api"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/scheduler/framework"

	"math"
)

type allocateAction struct {
	ssn *framework.Session
}

func New() *allocateAction {
	return &allocateAction{}
}

func (alloc *allocateAction) Name() string {
	return "allocate"
}

func (alloc *allocateAction) Initialize() {}

func (alloc *allocateAction) Execute(ssn *framework.Session) {
	glog.V(3).Infof("Enter Allocate...")
	defer glog.V(3).Infof("Leaving Allocate...")

	// Load configuration of policy
	policyConf := ssn.GetPolicy("kube-system/scheduler-conf")
	glog.V(3).Infof("Using policy %v.", policyConf)

	policyFn := fifoRandomFn
	switch policyConf {
	case "fifoRandom":
		policyFn = fifoRandomFn
	case "fifoHeter":
		policyFn = fifoHeterFn
	case "sjfHeter":
		policyFn = sjfHeterFn
	case "custom":
		policyFn = customFn
	}

	// Prepare job queue
	jobs := []*api.JobInfo{}
	minRequired := math.MaxInt32
	var t *api.TaskInfo
	for _, job := range ssn.Jobs {
		if len(job.TaskStatusIndex[api.Pending]) >= job.MinAvailable {
			jobs = append(jobs, job)
			if len(job.TaskStatusIndex[api.Pending]) < minRequired {
				minRequired = len(job.TaskStatusIndex[api.Pending])
			}
			if t == nil {
				for _, task := range job.TaskStatusIndex[api.Pending] {
					t = task
					break
				}
			}
		} else {
			glog.V(3).Infof("Job <%v, %v> has %v tasks pending but requires %v tasks.",
				job.Namespace, job.Name, len(job.TaskStatusIndex[api.Pending]), job.MinAvailable)
		}
	}

	if len(jobs) == 0 {
		glog.V(3).Infof("No jobs awaiting, skipping policy")
		return
	}

	glog.V(3).Infof("%v jobs awaiting:", len(jobs))
	for _, job := range jobs {
		glog.V(3).Infof("    <%v/%v>", job.Namespace, job.Name)
	}

	// Prepare node info
	nodes := []*api.NodeInfo{}
	nodeCount := 0
	selector := labels.SelectorFromSet(labels.Set(map[string]string{"type": "virtual-kubelet"}))
	for _, node := range ssn.Nodes {
		if selector.Matches(labels.Set(node.Node.Labels)) {
			nodes = append(nodes, node)
			if t.Resreq.LessEqual(node.Idle) {
				nodeCount = nodeCount + 1
			}
		}
	}

	glog.V(3).Infof("%v/%v nodes available:", nodeCount, len(nodes))
	for _, node := range nodes {
		glog.V(3).Infof("    <%v>", node.Name)
	}

	if nodeCount < minRequired {
		glog.V(3).Infof("Not enough node (%v) for any job (%v), skipping policy", nodeCount, minRequired)
		return
	}

	// Call policy function to get allocation for first job
	allocation := policyFn(jobs, nodes)

	for len(allocation) != 0 {

		// Check allocation to get a clean (possible) placement
		cleaned := make(map[*api.TaskInfo]*api.NodeInfo)
		used := make(map[*api.NodeInfo]bool)
		for idx, job := range jobs {
			allocated := true
			first := true
			tempused := make(map[*api.NodeInfo]bool)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				node, ok := allocation[task]
				if ok && (!task.Resreq.LessEqual(node.Idle) || used[node] || tempused[node]) {
					glog.Errorf("Not enough idle resource on %v to bind Task <%v/%v> in Session %v",
						node.Name, task.Namespace, task.Name, ssn.UID)
					ok = false
				}
				if !ok && !first && allocated {
					allocated = false
					glog.Errorf("Job <%v/%v> partially allocated, ignored", job.Namespace, job.Name)
					break
				} else if !ok {
					allocated = false
				} else if !allocated {
					glog.Errorf("Job <%v/%v> partially allocated, ignored", job.Namespace, job.Name)
					break
				}
				tempused[node] = true
				first = false
			}
			if allocated {
				for _, task := range job.TaskStatusIndex[api.Pending] {
					cleaned[task] = allocation[task]
					used[allocation[task]] = true
				}
				jobs = append(jobs[: idx], jobs[idx + 1 :]...)
				nodeCount = nodeCount - len(cleaned)
				glog.Infof("Exactly 1 job found in allocation, ignoring the rest (if any)")
				break; // Allocate tasks of one job at a time
			}
		}

		// Allocate tasks
		for task, node := range cleaned {
			glog.V(3).Infof("Try to bind Task <%v/%v> to Node <%v>: <%v> vs. <%v>",
				task.Namespace, task.Name, node.Name, task.Resreq, node.Idle)

			// Allocate idle resource to the task.
			glog.V(3).Infof("Binding Task <%v/%v> to node <%v>",
				task.Namespace, task.Name, node.Name)
			if err := ssn.Allocate(task, node.Name); err != nil {
				glog.Errorf("Failed to bind Task %v on %v in Session %v",
					task.UID, node.Name, ssn.UID)
			} else {
				ssn.UpdateScheduledTime(task)
			}
		}

		glog.V(3).Infof("%v jobs awaiting:", len(jobs))
		for _, job := range jobs {
			glog.V(3).Infof("    <%v/%v>", job.Namespace, job.Name)
		}

		glog.V(3).Infof("%v/%v nodes available:", nodeCount, len(nodes))
		for _, node := range nodes {
			glog.V(3).Infof("    <%v>", node.Name)
		}

		// Call policy function to get allocation for next job
		allocation = policyFn(jobs, nodes)

	}

}

func (alloc *allocateAction) UnInitialize() {}
