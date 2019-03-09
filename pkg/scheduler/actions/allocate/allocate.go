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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/golang/glog"

	"github.com/kubernetes-incubator/kube-arbitrator/pkg/scheduler/api"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/scheduler/framework"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/scheduler/util"

	"strconv"
	"os"
	"fmt"
	"encoding/json"
	"reflect"
	"sort"
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

func jobOrderFn(l, r interface{}) int {
	lv := l.(*api.JobInfo)
	rv := r.(*api.JobInfo)
	lc := metav1.Now()
	rc := metav1.Now()
	for _, lt := range lv.TaskStatusIndex[api.Pending] {
		if lt.Pod.ObjectMeta.CreationTimestamp.Before(&lc) {
			lc = lt.Pod.ObjectMeta.CreationTimestamp
		}
	}
	for _, rt := range rv.TaskStatusIndex[api.Pending] {
		if rt.Pod.ObjectMeta.CreationTimestamp.Before(&rc) {
			rc = rt.Pod.ObjectMeta.CreationTimestamp
		}
	}
	if lc.Before(&rc) {
		glog.V(3).Infof("%s (%v) before %s (%v)", lv.Name, lc, rv.Name, rc)
		return -1
	}
		glog.V(3).Infof("%s (%v) before %s (%v)", rv.Name, rc, lv.Name, lc)
	return 1
}

func atoi(str string) int {
	i,_ := strconv.Atoi(str)
	return i
}

func jobNum(job *api.JobInfo) int {
	// TODO: we assume that jobs are named 'job-NN'
	return atoi(job.Name[4:])
}

func nodeNum(nodeName string) int {
	// TODO: we assume that nodes are named 'vk-N'
	return atoi(nodeName[3:])
}

func prepareInput(jobs []*api.JobInfo, nodes []*api.NodeInfo, nodesAvailable map[string]*api.NodeInfo) InputT {
	var input InputT

	// Collect rack capacities and number of GPU racks from node info
	rackCap := make(map[int]int)
	for _, node := range nodes {
		if rack, found := node.Node.ObjectMeta.Labels["Rack"]; found {
			rackID, _ := strconv.Atoi(rack)
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
		queueJob.JobID = jobNum(job)
		queueJob.K = job.MinAvailable
		for _, task := range job.TaskStatusIndex[api.Pending] {
			queueJob.JobType = task.Pod.ObjectMeta.Labels["type"]
			queueJob.Duration = atoi(task.Pod.ObjectMeta.Labels["FastDuration"])
			queueJob.SlowDuration = atoi(task.Pod.ObjectMeta.Labels["SlowDuration"])
			break
		}
		input.Queue = append(input.Queue, queueJob)
	}

	// Collect node info
	for nodeName, _ := range nodesAvailable {
		input.Machines = append(input.Machines, nodeNum(nodeName))
	}

	sort.Ints(input.Machines)

	return input
}

// keep track of input and output in the previous allocation decision
var prevInput InputT
var prevOutput OutputT

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
	ssn.AddJobOrderFn(jobOrderFn)
	jobQueue := util.NewPriorityQueue(ssn.JobOrderFn)
	var trace string
	var t *api.TaskInfo
	for _, job := range ssn.Jobs {
		numPendingTasks := len(job.TaskStatusIndex[api.Pending])
		if numPendingTasks >= job.MinAvailable {
			jobQueue.Push(job)
			if t == nil {
				for _, task := range job.TaskStatusIndex[api.Pending] {
					// TODO assume that all the jobs belong to the same trace
					trace = task.Pod.ObjectMeta.Labels["trace"]
					t = task
					break
				}
			}
		} else {
			glog.V(3).Infof("Job <%v, %v> has %v tasks pending but requires %v tasks (creation in progress?).",
				job.Namespace, job.Name, numPendingTasks, job.MinAvailable)
		}
	}

	if jobQueue.Empty() {
		glog.V(3).Infof("No jobs awaiting, DONE")
		return
	}

	jobs := []*api.JobInfo{}
	for {
		job := jobQueue.Pop()
		jobs = append(jobs, job.(*api.JobInfo))
		if jobQueue.Empty() {
			break
		}
	}

	glog.V(3).Infof("%v jobs awaiting:", len(jobs))
	for _, job := range jobs {
		glog.V(3).Infof("    <%v/%v>", job.Namespace, job.Name)
	}

	// Prepare node info
	nodes := []*api.NodeInfo{}
	nodesAvailable := make(map[string]*api.NodeInfo)
	selector := labels.SelectorFromSet(labels.Set(map[string]string{"type": "virtual-kubelet"}))
	for _, node := range ssn.Nodes {
		if selector.Matches(labels.Set(node.Node.Labels)) {
			nodes = append(nodes, node)
			if t.Resreq.LessEqual(node.Idle) {
				nodesAvailable[node.Node.ObjectMeta.Name] = node
			}
		}
	}

	if len(nodesAvailable) <= 0 {
		glog.V(3).Infof("No nodes available, DONE")
		return
	}

	glog.V(3).Infof("%v/%v nodes available:", len(nodesAvailable), len(nodes))
	for _, node := range nodes {
		if _, found := nodesAvailable[node.Name]; found {
			glog.V(3).Infof("    <%v>: available", node.Name)
		} else {
			glog.V(3).Infof("    <%v>", node.Name)
		}
	}

	nothingScheduled := true
	var input InputT

	for { // repeat until no more jobs can be scheduled (one job per iteration)
		// Prepare policy input for grader json
		input = prepareInput(jobs, nodes, nodesAvailable)

		// Call policy function to get allocation
		allocation := policyFn(jobs, nodes)

		if len(allocation) == 0 { // nothing could be scheduled
			break
		}

		nothingScheduled = false

		// Validate allocation returned by the policy
		var jobAllocated *api.JobInfo
		var jobAllocatedIdx int
		validAllocation := true
		// Tasks don't include reference to job, so need to traverse all jobs and tasks
		for idx, job := range jobs {
			nodeInUse := make(map[*api.NodeInfo]bool)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				node, taskAllocated := allocation[task]
				if taskAllocated { // task found in allocation
					if jobAllocated == nil {
						jobAllocated = job // we found the job
						jobAllocatedIdx = idx
					} else { // we already found allocated task before, check if they match
						if job != jobAllocated { // allocated included multiple jobs
							validAllocation = false
							glog.Errorf("ERROR! Allocation included both Job %v and %v.",
								jobAllocated.Name, job.Name)
							break
						}
					}
					if nodeInUse[node] {
						validAllocation = false
						glog.Errorf("ERROR! Could not allocate Task <%v/%v>: Node %v already in use",
							task.Namespace, task.Name, node.Name)
						break
					}
					if !task.Resreq.LessEqual(node.Idle) {
						validAllocation = false
						glog.Errorf("ERROR! Could not allocate Task <%v/%v>: node enough idle resources in Node %v",
							task.Namespace, task.Name, node.Name)
						break
					}
					nodeInUse[node] = true
				} else { // task not allocated by the policy
					if jobAllocated != nil { // some task from this job was allocated, but this task wasn't
						validAllocation = false
						glog.Errorf("ERROR! Job %v partially allocated", job.Name)
						break
					} else {
						// can contiue to the next task 
						// not skipping the entire job, to detect partial allocations
						continue
					}
				}
			}
			if jobAllocated != nil { // allocation included task(s) from this job
				break // no need to check other jobs
			}
		}
		// prepare output for grader
		var output OutputT
		if jobAllocated != nil {
			output.JobID = jobNum(jobAllocated)
			// find nodes in the returned allocation that belong to <jobAllocated>
			for _, task := range jobAllocated.TaskStatusIndex[api.Pending] {
				node, found := allocation[task]
				if found {
					nodeID := nodeNum(node.Node.ObjectMeta.Name)
					output.Machines = append(output.Machines, nodeID)
				}
			}
		}

		// Record scheduling decision in a json file
		recordDecision(input,output,trace)

		if validAllocation {
			allocated := false
			// Allocate tasks
			for task, node := range allocation {
				glog.V(3).Infof("Try to bind Task <%v/%v> to Node <%v>: <%v> vs. <%v>",
					task.Namespace, task.Name, node.Name, task.Resreq, node.Idle)
				if err := ssn.Allocate(task, node.Name); err != nil {
					glog.Errorf("ERROR! Failed to bind Task %v on %v in Session %v",
						task.UID, node.Name, ssn.UID)
				} else {
					ssn.UpdateScheduledTime(task)
					// if we succeeded with at least one task, we shouldn't try scheduling this job again
					allocated = true
					// update nodesAvailable for next iteration
					delete(nodesAvailable, node.Name)
				}
			}
			if allocated {
				// Update jobs for next iteration
				jobs = append(jobs[:jobAllocatedIdx], jobs[jobAllocatedIdx+1:]...)
			}
		}

		// if no more jobs or nodes, exit the loop
		if len(jobs) == 0 {
			glog.V(3).Infof("No jobs awaiting, DONE")
			break
		}

		glog.V(3).Infof("%v jobs awaiting:", len(jobs))
		for _, job := range jobs {
			glog.V(3).Infof("    <%v/%v>", job.Namespace, job.Name)
		}

		if len(nodesAvailable) <= 0 {
			glog.V(3).Infof("No nodes available, DONE")
			break
		}

		glog.V(3).Infof("%v/%v nodes available:", len(nodesAvailable), len(nodes))
		for _, node := range nodes {
			if _, found := nodesAvailable[node.Name]; found {
				glog.V(3).Infof("    <%v>: available", node.Name)
			} else {
				glog.V(3).Infof("    <%v>", node.Name)
			}
		}
	}
	if nothingScheduled { // if nothing scheduled, record empty scheduling decision
		var output OutputT // empty
		recordDecision(input,output,trace)
	}
}

func recordDecision(input InputT, output OutputT, trace string) {
	// Marshal policy input and output to json and write to file
	var message Message
	message.Input = input
	if len(output.Machines)>0 {
		sort.Ints(output.Machines)
		message.Output = output
	}
	// save only if input is different than the previous one
	if !reflect.DeepEqual(input,prevInput) || !reflect.DeepEqual(output,prevOutput) {
		jobsInfo := []int{}
		for _,jq  := range(input.Queue) {
			jobsInfo = append(jobsInfo, jq.JobID)
		}
		sort.Ints(jobsInfo)
		nodesInfo := input.Machines
		sort.Ints(nodesInfo)
		if len(output.Machines)>0 {
			glog.Infof("Policy scheduled JobID=%v to %v (Input queue: %v, nodes: %v)",
				output.JobID, output.Machines, jobsInfo, nodesInfo)
		} else {
			glog.Infof("Policy could not schedule any job (Input queue: %v, nodes: %v)",
				jobsInfo, nodesInfo)
		}
		b, _ := json.Marshal(message)
		traceFile, _ := os.OpenFile(fmt.Sprintf("/tmp/trace-%s.json", trace), os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
		traceFile.Write(append(b, ','))
		traceFile.Close()
	} else {
		glog.V(3).Infof("Same input, skip recording")
	}
	// remember input and output, to avoid saving identical scheduling decisions
	prevInput = input
	prevOutput = output
}

func (alloc *allocateAction) UnInitialize() {}
