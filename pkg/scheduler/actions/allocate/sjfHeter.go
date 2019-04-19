package allocate

import (
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
)

func sjfHeterFn(jobs []*api.JobInfo, nodes []*api.NodeInfo) map[*api.TaskInfo]*api.NodeInfo {
	allocation := make(map[*api.TaskInfo]*api.NodeInfo)
	i := 0
	for _, job := range jobs {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			if i >= len(nodes) {
				break
			}
			allocation[task] = nodes[i]
			i++
		}
	}
	return allocation
}
