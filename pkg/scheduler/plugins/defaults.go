/*
Copyright 2019 The Kubernetes Authors.

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

package plugins

import "github.com/kubernetes-sigs/kube-batch/pkg/scheduler/conf"

// ApplyPluginConfDefaults sets option's filed to its default value if not set
func ApplyPluginConfDefaults(option *conf.PluginOption) {
	t := true

	if option.EnabledJobOrder == nil {
		option.EnabledJobOrder = &t
	}
	if option.EnabledJobReady == nil {
		option.EnabledJobReady = &t
	}
	if option.EnabledTaskOrder == nil {
		option.EnabledTaskOrder = &t
	}
	if option.EnabledPreemptable == nil {
		option.EnabledPreemptable = &t
	}
	if option.EnabledReclaimable == nil {
		option.EnabledReclaimable = &t
	}
	if option.EnabledQueueOrder == nil {
		option.EnabledQueueOrder = &t
	}
	if option.EnabledPredicate == nil {
		option.EnabledPredicate = &t
	}
	if option.EnabledNodeOrder == nil {
		option.EnabledNodeOrder = &t
	}
}
