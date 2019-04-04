// Copyright 2019 eBay Inc.
// Primary authors: Simon Fell, Diego Ongaro,
//                  Raymond Kroeker, and Sathish Kandasamy.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubediscovery

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/ebay/beam/discovery"
	"github.com/ericchiang/k8s"
	corev1 "github.com/ericchiang/k8s/apis/core/v1"
	metav1 "github.com/ericchiang/k8s/apis/meta/v1"
	"github.com/stretchr/testify/assert"
)

func Test_NewLocator(t *testing.T) {
	assert := assert.New(t)
	mock := new(mockClient)
	kube := newKubeClient(mock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loc, err := kube.NewLocator(ctx, "grpc", "beam/type in (diskview)")
	assert.NoError(err)
	assert.Equal(uint64(0), loc.Cached().Version)
	assert.Equal("v=0[]", stringify(loc.Cached()))

	diskviewPorts := map[string]int32{"grpc": 9980}
	pod := mockPod{name: "diskview-1", typeName: "diskview", ports: diskviewPorts,
		status: "True"}.build()
	mock.pods = append(mock.pods, pod)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	loc, err = kube.NewLocator(ctx, "grpc", "beam/type in (diskview)")
	assert.NoError(err)
	assert.Equal(uint64(1), loc.Cached().Version)
	assert.Equal("v=1[{tcp://diskview-1:9980}]", stringify(loc.Cached()))

	// Test NewLocator with canceled context.
	ctx, cancel = context.WithCancel(context.Background())
	// Deliberately cancel the ctx.
	cancel()
	loc, err = kube.NewLocator(ctx, "grpc", "beam/type in (diskview)")
	assert.NoError(err)
	assert.Equal(uint64(0), loc.Cached().Version)
	assert.Equal("v=0[]", stringify(loc.Cached()))
}

// Test context on discoverLoop method.
func Test_discoverLoop_context(t *testing.T) {
	assert := assert.New(t)
	mock := new(mockClient)
	kube := newKubeClient(mock)

	locator := &kubeLocator{
		client:   kube,
		portName: "kafka",
		selector: "beam/type in (blog)",
		current:  discovery.NewStaticLocator(nil),
	}
	// Test context cancel.
	ctx, cancel := context.WithCancel(context.Background())
	// Deliberately cancel the ctx.
	cancel()
	err := locator.discoverLoop(ctx)
	assert.EqualError(err, "context canceled")
	// Test context expiry.
	start := time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = locator.discoverLoop(ctx)
	assert.True(time.Since(start) >= time.Millisecond,
		"discoverLoop didn't block long enough")
	assert.Equal(err, ctx.Err())
}

func Test_tryDiscover(t *testing.T) {
	diskviewPorts := map[string]int32{"grpc": 9980}

	var tests = []struct {
		name         string
		portName     string
		setup        []*corev1.Pod
		afterSetup   []*corev1.Pod
		expected     string
		matchAddress bool
	}{
		{
			name:       "empty",
			portName:   "grpc",
			setup:      []*corev1.Pod{},
			afterSetup: []*corev1.Pod{},
			expected:   "v=0[]",
		},
		{
			name:     "single_endpoint",
			portName: "grpc",
			setup:    []*corev1.Pod{},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview", ports: diskviewPorts,
					features: []string{"A", "B", "SP"}, partition: 1, numPartitions: 2,
					status: "True"}.build(),
			},
			expected: "v=1[{tcp://diskview-1:9980|p=1|pc=2|f=A,B,SP}]",
		},
		{
			name:     "endpoint_with_diff_features",
			portName: "grpc",
			setup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, features: []string{"A", "B", "SP"},
					status: "True"}.build(),
			},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, features: []string{"C", "D", "PO"},
					status: "True"}.build(),
			},
			expected: "v=2[{tcp://diskview-1:9980|f=C,D,PO}]",
		},
		{
			name:     "endpoint_with_diff_partition",
			portName: "grpc",
			setup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, partition: 1, numPartitions: 2,
					status: "True"}.build(),
			},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, partition: 2, numPartitions: 2,
					status: "True"}.build(),
			},
			expected: "v=2[{tcp://diskview-1:9980|p=2|pc=2}]",
		},
		{
			name:     "endpoint_with_diff_paritition_count",
			portName: "grpc",
			setup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, partition: 1, numPartitions: 2,
					status: "True"}.build(),
			},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, partition: 1, numPartitions: 4,
					status: "True"}.build(),
			},
			expected: "v=2[{tcp://diskview-1:9980|p=1|pc=4}]",
		},
		{
			name:     "different_in_order_endpoint",
			portName: "grpc",
			setup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
				mockPod{name: "diskview-2", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
			},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-2", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
			},
			expected:     "v=1[{tcp://diskview-1:9980}{tcp://diskview-2:9980}]",
			matchAddress: true,
		},
		{
			name:     "endpoint_removed",
			portName: "grpc",
			setup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
				mockPod{name: "diskview-2", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
			},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-2", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
			},
			expected:     "v=2[{tcp://diskview-2:9980}]",
			matchAddress: true,
		},
		{
			name:     "endpoint_replaced",
			portName: "grpc",
			setup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
				mockPod{name: "diskview-2", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
			},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-3", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
				mockPod{name: "diskview-4", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
			},
			expected: "v=2[{tcp://diskview-3:9980}{tcp://diskview-4:9980}]",
		},
		{
			name:     "all_endpoints_removed",
			portName: "grpc",
			setup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
				mockPod{name: "diskview-2", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
			},
			afterSetup: []*corev1.Pod{},
			expected:   "v=2[]",
		},
		{
			name:     "pod_condition_Type_False",
			portName: "grpc",
			setup:    []*corev1.Pod{},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, status: "True"}.build(),
				mockPod{name: "diskview-2", typeName: "diskview",
					ports: diskviewPorts, status: "False"}.build(),
			},
			expected: "v=1[{tcp://diskview-1:9980}]",
		},
		{
			name:     "pod_ip_empty",
			portName: "grpc",
			setup:    []*corev1.Pod{},
			afterSetup: []*corev1.Pod{
				mockPod{name: "diskview-1", typeName: "diskview",
					ports: diskviewPorts, status: "True", emptyIP: true}.build(),
			},
			expected: "v=0[]",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert := assert.New(t)

			mock := new(mockClient)
			kube := newKubeClient(mock)
			locator := &kubeLocator{
				client:   kube,
				portName: test.portName,
				selector: "beam/type in (diskview)",
				current:  discovery.NewStaticLocator(nil),
			}
			// Setup
			mock.pods = append(mock.pods, test.setup...)
			locator.tryDiscover(context.Background())
			r1 := locator.Cached()
			// afterSetup
			mock.pods = nil
			mock.pods = append(mock.pods, test.afterSetup...)
			locator.tryDiscover(context.Background())
			r2 := locator.Cached()
			assert.Equal(test.expected, stringify(r2))
			// Check Endpoint address
			m := make(map[string]*discovery.Endpoint)
			for _, e := range r1.Endpoints {
				m[e.HostPort()] = e
			}
			if test.matchAddress {
				for i := range r2.Endpoints {
					if e, ok := m[r2.Endpoints[i].HostPort()]; ok {
						assert.True(e == r2.Endpoints[i], "pointer address should match")
					}
				}
			}
		})
	}
}

func Test_KubeLocator_String(t *testing.T) {
	assert := assert.New(t)
	locator := &kubeLocator{
		portName: "grpc",
		selector: "beam/type in (diskview)",
		current:  discovery.NewStaticLocator(nil),
	}
	assert.Equal("Empty KubeLocator(portName=grpc, selector={beam/type in (diskview)})",
		locator.String())

	locator.current.Set([]*discovery.Endpoint{
		&discovery.Endpoint{Network: "tcp", Host: "diskview-1", Port: "9980"},
	})
	assert.Equal("KubeLocator(portName=grpc, selector={beam/type in (diskview)}, tcp://diskview-1:9980)",
		locator.String())

	locator.current.Set([]*discovery.Endpoint{
		&discovery.Endpoint{Network: "tcp", Host: "diskview-1", Port: "9980"},
		&discovery.Endpoint{Network: "tcp", Host: "diskview-2", Port: "9980"},
	})
	assert.Equal("KubeLocator(portName=grpc, selector={beam/type in (diskview)}, len=2, first=tcp://diskview-1:9980)",
		locator.String())
}

// The behavior of mapsEqual is tested against this.
func slowMapsEqual(x, y map[string]string) bool {
	if x == nil {
		x = make(map[string]string)
	}
	if y == nil {
		y = make(map[string]string)
	}
	return reflect.DeepEqual(x, y)
}

func Test_mapsEqual_quickCheck(t *testing.T) {
	err := quick.CheckEqual(slowMapsEqual, mapsEqual, nil)
	assert.NoError(t, err)
}

func Test_mapsEqual(t *testing.T) {
	assert := assert.New(t)

	// nil vs empty is an edge case worth covering explicitly.
	assert.True(mapsEqual(nil, map[string]string{}))
	assert.True(mapsEqual(map[string]string{}, nil))

	var maps = []map[string]string{
		nil,
		map[string]string{"": "Bob"},
		map[string]string{"bob": ""},
		map[string]string{"bob": "Alice"},
		map[string]string{"bob": "Bob", "alice": ""},
		map[string]string{"bob": "Bob", "alice": "Alice"},
		map[string]string{"bob": "Bob", "henry": ""},
		map[string]string{"bob": "Bob", "henry": "henry"},
		map[string]string{"bob": "Bob"},
		map[string]string{},
	}
	for _, x := range maps {
		for _, y := range maps {
			assert.Equal(slowMapsEqual(x, y), mapsEqual(x, y),
				"x=%#v, y=%#v", x, y)
		}
	}
}

func stringify(result discovery.Result) string {
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("v=%d[", result.Version))
	for _, e := range result.Endpoints {
		b.WriteString("{")
		b.WriteString(e.String())
		if val, exists := e.Annotations["beam/partition"]; exists {
			b.WriteString(fmt.Sprintf("|p=%v", val))
		}
		if val, exists := e.Annotations["beam/partitionCount"]; exists {
			b.WriteString(fmt.Sprintf("|pc=%v", val))
		}
		if val, exists := e.Annotations["beam/features"]; exists {
			b.WriteString(fmt.Sprintf("|f=%v", val))
		}
		b.WriteString("}")
	}
	b.WriteString("]")
	return b.String()
}

func newKubeClient(mock *mockClient) *KubeClient {
	kube := &KubeClient{
		client:    mock,
		namespace: "testing",
	}
	return kube
}

// mockClient implements k8sClient.
type mockClient struct {
	pods []*corev1.Pod
}

// List implements k8sClient.List. It doesn't support Kubernetes label selector.
func (k8s *mockClient) List(ctx context.Context, namespace string,
	out k8s.ResourceList, options ...k8s.Option) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	pods := out.(*corev1.PodList)
	pods.Items = append([]*corev1.Pod(nil), k8s.pods...)
	return nil
}

type mockPod struct {
	name          string
	typeName      string
	features      []string
	partition     int
	numPartitions int
	ports         map[string]int32
	status        string
	emptyIP       bool
}

func (spec mockPod) build() *corev1.Pod {
	podStatus := "Running"
	ready := "Ready"
	protocol := "TCP"

	var podIP string
	if !spec.emptyIP {
		podIP = spec.name
	}

	// Build annotations.
	a := map[string]string{}

	set := func(key string, value int) {
		if value > 0 {
			a[key] = strconv.Itoa(value)
		}
	}
	set("beam/partition", spec.partition)
	set("beam/partitionCount", spec.numPartitions)
	if len(spec.features) > 0 {
		a["beam/features"] = strings.Join(spec.features, ",")
	}

	pod := &corev1.Pod{
		Metadata: &metav1.ObjectMeta{
			Name: &spec.name,
			Labels: map[string]string{
				"beam/type": spec.typeName,
			},
			Annotations: a,
		},
		Spec: &corev1.PodSpec{
			Containers: []*corev1.Container{
				&corev1.Container{
					Ports: make([]*corev1.ContainerPort, len(spec.ports)),
				},
			},
		},
		Status: &corev1.PodStatus{
			PodIP: &podIP,
			Phase: &podStatus,
			Conditions: []*corev1.PodCondition{
				&corev1.PodCondition{
					Type:   &ready,
					Status: &spec.status,
				},
			},
		},
	}
	i := 0
	for name, port := range spec.ports {
		pod.Spec.Containers[0].Ports[i] = &corev1.ContainerPort{
			Name:          &name,
			ContainerPort: &port,
			Protocol:      &protocol,
		}
		i++
	}
	return pod
}
