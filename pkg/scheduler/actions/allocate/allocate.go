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
	"strconv"
	"os"
	"fmt"
	"encoding/json"
)

type JobT struct {
	JobID        int    `json:"jobID"`
	JobType      string `json:"jobType"`
	K            int    `json:"k"`
	Duration     int    `json:"duration"`
	SlowDuration int    `json:"slowDuration"`
}

type InputT struct {
	RackCap              []int    `json:"rack_cap"`
	NumLargeMachineRacks int      `json:"numLargeMachineRacks"`
	Queue                []JobT   `json:"queue"`
	Machines             []int    `json:"machines"`
}

type OutputT struct {
	JobID    int   `json:"jobID"`
	Machines []int `json:"machines"`
}

type Message struct {
	Input  InputT `json:"input"`
	Output interface{} `json:"output"`
}

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

func prepareInput(jobs []*api.JobInfo, nodes []*api.NodeInfo, nodesAvailable map[int]*api.NodeInfo) InputT {
	var input InputT

	// Collect rack capacities and number of GPU racks from node info
	rackCap := make(map[int]int)
	for _, node := range nodes {
		if rack, found := node.Node.ObjectMeta.Labels["Rack"]; found {
			rackID, _ := strconv.ParseInt(rack, 10, 64)
			if _, found = rackCap[int(rackID)]; found {
				rackCap[int(rackID)] = rackCap[int(rackID)] + 1
			} else {
				rackCap[int(rackID)] = 1
			}
			if gpu, found := node.Node.ObjectMeta.Labels["GPU"]; found && gpu == "true" {
				if int(rackID) > input.NumLargeMachineRacks {
					input.NumLargeMachineRacks = int(rackID)
				}
			}
		}
	}
	for rackID := 1; rackID <= len(rackCap); rackID ++ {
		input.RackCap = append(input.RackCap, rackCap[rackID])
	}

	// Collect job info
	for _, job := range jobs {
		var queueJob JobT
		jobID, _ := strconv.ParseInt(job.Name[4 :], 10, 64)
		queueJob.JobID = int(jobID)
		queueJob.K = job.MinAvailable
		for _, task := range job.TaskStatusIndex[api.Pending] {
			queueJob.JobType = task.Pod.ObjectMeta.Labels["type"]
			fastDuration, _ := strconv.ParseInt(task.Pod.ObjectMeta.Labels["FastDuration"], 10, 64)
			queueJob.Duration = int(fastDuration)
			slowDuration, _ := strconv.ParseInt(task.Pod.ObjectMeta.Labels["SlowDuration"], 10, 64)
			queueJob.SlowDuration = int(slowDuration)
			break
		}
		input.Queue = append(input.Queue, queueJob)
	}

	// Collect node info
	for nodeID, _ := range nodesAvailable {
		input.Machines = append(input.Machines, nodeID)
	}

	return input
}

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
	var trace string
	var t *api.TaskInfo
	for _, job := range ssn.Jobs {
		if len(job.TaskStatusIndex[api.Pending]) >= job.MinAvailable {
			jobs = append(jobs, job)
			if len(job.TaskStatusIndex[api.Pending]) < minRequired {
				minRequired = len(job.TaskStatusIndex[api.Pending])
			}
			if t == nil {
				for _, task := range job.TaskStatusIndex[api.Pending] {
					trace = task.Pod.ObjectMeta.Labels["trace"]
					t = task
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
	nodesAvailable := make(map[int]*api.NodeInfo)
	selector := labels.SelectorFromSet(labels.Set(map[string]string{"type": "virtual-kubelet"}))
	for _, node := range ssn.Nodes {
		if selector.Matches(labels.Set(node.Node.Labels)) {
			nodes = append(nodes, node)
			if t.Resreq.LessEqual(node.Idle) {
				nodeIdx, _ := strconv.ParseInt(node.Node.ObjectMeta.Name[3 :], 10, 64)
				nodesAvailable[int(nodeIdx)] = node
			}
		}
	}

	glog.V(3).Infof("%v/%v nodes available:", len(nodesAvailable), len(nodes))
	for _, node := range nodes {
		glog.V(3).Infof("    <%v>", node.Name)
	}

	if len(nodesAvailable) < minRequired {
		glog.V(3).Infof("Not enough node (%v) for any job (%v), skipping policy", len(nodesAvailable), minRequired)
		return
	}

	// Prepare policy input for grader json
	input := prepareInput(jobs, nodes, nodesAvailable)

	// Call policy function to get allocation for first job
	allocation := policyFn(jobs, nodes)

	for len(allocation) != 0 {
	        var output OutputT

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
				jobID, _ := strconv.ParseInt(job.Name[4 :], 10, 64)
				output.JobID = int(jobID)
				for _, task := range job.TaskStatusIndex[api.Pending] {
					nodeID, _ := strconv.ParseInt(allocation[task].Node.ObjectMeta.Name[3 :], 10, 64)
					output.Machines = append(output.Machines, int(nodeID))
					cleaned[task] = allocation[task]
					used[allocation[task]] = true
					delete(nodesAvailable, int(nodeID))
				}
				jobs = append(jobs[: idx], jobs[idx + 1 :]...)
				glog.Infof("Exactly 1 job found in allocation, ignoring the rest (if any)")
				break; // Allocate tasks of one job at a time
			}
		}

		// Marshal policy input and output to json and write to file
		var message Message
		message.Input = input
		if len(output.Machines) != 0 {
			message.Output = output
		}
		b, _ := json.Marshal(message)
		var traceFile *os.File
		if fi, err := os.Stat(fmt.Sprintf("/tmp/trace-%s.json", trace)); err == nil {
			traceFile, _ = os.OpenFile(fmt.Sprintf("/tmp/trace-%s.json", trace), os.O_WRONLY, 0644)
			traceFile.Seek(fi.Size() - 1, 0)
			traceFile.Write([]byte(","))
		} else if os.IsNotExist(err) {
			traceFile, _ = os.Create(fmt.Sprintf("/tmp/trace-%s.json", trace))
			traceFile.Write([]byte("["))
		}
		traceFile.Write(append(b, ']'))
		traceFile.Close()

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

		glog.V(3).Infof("%v/%v nodes available:", len(nodesAvailable), len(nodes))
		for _, node := range nodes {
			glog.V(3).Infof("    <%v>", node.Name)
		}

		// Prepare policy input for grader json
		input = prepareInput(jobs, nodes, nodesAvailable)

		// Call policy function to get allocation for next job
		allocation = policyFn(jobs, nodes)

	}

}

func (alloc *allocateAction) UnInitialize() {}
