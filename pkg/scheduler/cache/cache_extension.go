package cache

import (
	"time"
	"fmt"

	"github.com/golang/glog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	arbapi "github.com/kubernetes-incubator/kube-arbitrator/pkg/scheduler/api"
)

func (sc *SchedulerCache) UpdateScheduledTime(task *arbapi.TaskInfo) error {
	sc.Mutex.Lock()
        defer sc.Mutex.Unlock()

	updateInterval := time.Second
	retryTimes := 5
	scheduledTime := metav1.NewTime(time.Now())
	for i :=0; i < retryTimes; i++ {
		pod, err := sc.kubeclient.CoreV1().Pods(task.Pod.Namespace).Get(task.Pod.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}

		if pod.ObjectMeta.Annotations == nil {
			pod.ObjectMeta.Annotations = make(map[string]string)
		}
		pod.ObjectMeta.Annotations["scheduledTime"] = scheduledTime.Rfc3339Copy().String()

		pod, err = sc.kubeclient.CoreV1().Pods(pod.Namespace).Update(pod)
		if err == nil {
			return nil
		}
		if err != nil && !apierrors.IsConflict(err) {
			if apierrors.IsNotFound(err) {
				return nil
			}
			glog.Errorf("falied to update pod scheduled time: %v", err)
			return err
		}
		time.Sleep(updateInterval)
	}
	return fmt.Errorf("update pod schuduled time failed after %d retries", retryTimes)
}
