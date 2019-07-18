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

// Package kubediscovery provides an implementation of the Locator interface
// backed by Kubernetes service discovery.
package kubediscovery

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ericchiang/k8s"
	corev1 "github.com/ericchiang/k8s/apis/core/v1"
	"github.com/sirupsen/logrus"
)

// k8sClient defines the subset of k8s.Client used by KubeClient.
type k8sClient interface {
	// Implements k8s.Client.List.
	List(context.Context, string, k8s.ResourceList, ...k8s.Option) error
}

// KubeClient is a client to interact with the Kubernetes cluster. It's a
// connection to the Kubernetes API service in the same Kubernetes cluster as
// this container.
type KubeClient struct {
	client    k8sClient
	namespace string
}

// NewKubeClient creates a new KubeClient instance. It returns an error if its
// not running inside the Kubernetes cluster. The caller should create a single
// instance of KubeClient to create many Locators.
func NewKubeClient() (*KubeClient, error) {
	client, err := k8s.NewInClusterClient()
	if err != nil {
		return nil, err
	}
	kube := KubeClient{
		client:    client,
		namespace: client.Namespace,
	}
	return &kube, nil
}

// NewLocator creates a new Locator to discover Endpoints using Kubernetes
// service discovery.
//
// The Locator returns endpoints running in the same Kubernetes namespace as
// this process. To do so, it finds pods by the given label selector; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors.
// It searches each container within such pods and returns a discovery.Endpoint
// for each container's port that matches the given name.
//
// The returned Locator will stop receiving updates when the supplied context is
// canceled/expired.
//
// It returns an error when the client connection doesn't have enough privileges
// to communicate to Kubernetes API service or the given label selector syntax
// is invalid. Refer
// https://kubernetes.io/docs/reference/access-authn-authz/rbac/ to create
// Kubernetes Role-based-access-control settings to allow the process to find
// other pods.
func (client *KubeClient) NewLocator(ctx context.Context, portName string, selector string) (discovery.Locator, error) {
	locator := &kubeLocator{
		client:   client,
		portName: portName,
		selector: selector,
		current:  discovery.NewStaticLocator(nil),
	}
	// Try discover one time to validate access privileges and the label
	// selector syntax.
	err := locator.tryDiscover(ctx)
	if err != ctx.Err() {
		return nil, err
	}
	// Run discover in background.
	go locator.discoverLoop(ctx)
	return locator, nil
}

// kubeLocator is a Locator to discover endpoints in a Kubernetes cluster.
type kubeLocator struct {
	client   *KubeClient
	portName string
	selector string
	current  *discovery.StaticLocator
}

// discoverLoop calls tryDiscover to update endpoints periodically until
// supplied context is expired.
func (locator *kubeLocator) discoverLoop(ctx context.Context) error {
	for ctx.Err() == nil {
		err := locator.tryDiscover(ctx)
		if err != nil {
			logrus.WithError(err).Warn("Failed to discover endpoints from Kubernetes API service")
		}
		clocks.Wall.SleepUntil(ctx, clocks.Wall.Now().Add(time.Second*10))
	}
	return ctx.Err()
}

// tryDiscover polls Kubernetes for a single time to discover endpoints to
// update the locators results. It returns an error when unable to poll from
// Kubernetes API service.
func (locator *kubeLocator) tryDiscover(ctx context.Context) error {
	// Fetch pods from Kuberenetes.
	var pods corev1.PodList
	opt := k8s.QueryParam("labelSelector", locator.selector)
	err := locator.client.client.List(ctx, locator.client.namespace, &pods, opt)
	if err != nil {
		return err
	}

	// Extract discovery.Endpoints.
	var endpoints []*discovery.Endpoint
	for _, pod := range pods.Items {
		// Filter pods that are not yet allocated; see
		// https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.12/#podstatus-v1-core
		podIP := pod.GetStatus().GetPodIP()
		if podIP == "" {
			continue
		}
		// Filter pods by PodCondition 'Ready=True'; see
		// https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-conditions
		ready := false
		for _, c := range pod.GetStatus().GetConditions() {
			if c.GetType() == "Ready" && c.GetStatus() == "True" {
				ready = true
				break
			}
		}
		if !ready {
			continue
		}
		for _, container := range pod.Spec.Containers {
			for _, port := range container.Ports {
				if port.GetName() == locator.portName {
					e := discovery.Endpoint{
						Network:     strings.ToLower(port.GetProtocol()),
						Host:        podIP,
						Port:        strconv.Itoa(int(port.GetContainerPort())),
						Annotations: pod.Metadata.GetAnnotations(),
					}
					endpoints = append(endpoints, &e)
				}
			}
		}
	}

	// Update locator.current with changes.
	cached := locator.current.Cached().Endpoints
	existing := make(map[string]*discovery.Endpoint, len(cached))
	for _, e := range cached {
		existing[e.HostPort()] = e
	}
	update := len(endpoints) != len(cached)
	for i, e := range endpoints {
		prev, exists := existing[e.HostPort()]
		if exists && mapsEqual(prev.Annotations, e.Annotations) {
			endpoints[i] = prev
		} else {
			update = true
		}
	}
	if update {
		locator.current.Set(endpoints)
	}
	return nil
}

// mapsEqual reports whether map x and y are equal.
func mapsEqual(x, y map[string]string) bool {
	if len(x) != len(y) {
		return false
	}
	for xKey, xVal := range x {
		if yVal, exists := y[xKey]; !exists || xVal != yVal {
			return false
		}
	}
	return true
}

// Cached implements discovery.Locator.Cached.
func (locator *kubeLocator) Cached() discovery.Result {
	return locator.current.Cached()
}

// WaitForUpdate implements discovery.Locator.WaitForUpdate.
func (locator *kubeLocator) WaitForUpdate(ctx context.Context, oldVersion uint64) (discovery.Result, error) {
	return locator.current.WaitForUpdate(ctx, oldVersion)
}

// String implements discovery.Locator.String.
func (locator *kubeLocator) String() string {
	result := locator.current.Cached()
	switch len(result.Endpoints) {
	case 0:
		return fmt.Sprintf("Empty KubeLocator(portName=%v, selector={%v})", locator.portName, locator.selector)
	case 1:
		return fmt.Sprintf("KubeLocator(portName=%v, selector={%v}, %v)", locator.portName, locator.selector, result.Endpoints[0])
	default:
		return fmt.Sprintf("KubeLocator(portName=%v, selector={%v}, len=%v, first=%v)",
			locator.portName, locator.selector, len(result.Endpoints), result.Endpoints[0])
	}
}
