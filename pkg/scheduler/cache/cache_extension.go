package cache

import (
	"fmt"
	"time"

	"github.com/golang/glog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	arbapi "github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
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

func (sc *SchedulerCache) LoadSchedulerConf(path string) (map[string]string, error) {
	       ns, name, err := cache.SplitMetaNamespaceKey(path)
	       if err != nil {
		               return nil, err
		       }

		       confMap, err := sc.kubeclient.CoreV1().ConfigMaps(ns).Get(name, metav1.GetOptions{})
	       if err != nil {
		               return nil, err
		       }

		       return confMap.Data, nil
	}
