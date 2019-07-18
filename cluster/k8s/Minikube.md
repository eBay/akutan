## Minikube

### What is it?

[Minikube](https://github.com/kubernetes/minikube) is an implementation of Kubernetes
that can run in a VM on your local machine.

### Prerequisites

A hypervisor is required in which the cluster will run. Download a copy of
[VirtualBox](https://www.virtualbox.org/wiki/Downloads) to get going. Version
` 6.0.4,128413` is known to work. Homebrew also includes a cask:

```bash
$ brew cask install virtualbox
```

### Installation

Minikube can be installed from a Homebrew cask:

```bash
$ brew cask install minikube
```

### Bring Up Kubernetes

Start up the Kubernetes cluster in a virtual machine. The additional memory and
cores are recommended to run the Akutan pods. The additional disk space is
recommended to avoid having Kubernetes garbage-collect your Docker images.

```bash
$ minikube start --memory 5120 --cpus 4 --disk-size 40g
```

Minikube includes a dashboard with which you can interact with the locally running
cluster. To bring it up use:

```bash
$ minikube dashboard
```

### Build Docker Images

You can build your own images. It's easiest to build these using Minikube's
Docker daemon so that they don't need to be subsequently pulled into that Docker
daemon from some local registry. 

The images must be built from a Linux environment (to produce Linux binaries),
so it's best to build them from within the Akutan build image.

Go to the root directory of the repo and run the following:

```bash
$ make get # will clone the latest dependency tree
$ make docker-build-akutan-builder-in-minikube
$ make docker-build-akutan-service-in-minikube
```

If this is your first deploy since starting minikube, it might take a while as it
has to download all the docker images.


### Create Namespace In Kubernetes Cluster

Run the following to create `akutan-dev` namespace, where the Akutan services
will be deployed:

```bash
$ kubectl create namespace akutan-dev
```

### Create Access Control Rules For Service Discovery

Akutan services discover each other by reading the pod list using the Kubernetes
client API. This requires role-based access control settings to allow the pods
access to this information. Run:

```
$ kubectl apply -f cluster/k8s/rbac.yaml
```

### Deploy Akutan Services

Akutan Services are deployed via `kubectl` into the cluster using YAML
configuration files. These are templatized to control which images to run.

Run the following to generate the YAML configuration files and bring up the Akutan
services:

```bash
$ make get_install build # will rebuild tools needed to build the distribution
$ bin/gen-kube --akutan-images-path='' --akutan-images-tag='local' --logservice-image=akutan-kafka:local
$ kubectl apply -f cluster/k8s/generated
```

If all goes well, you can use `minikube service list` to find the endpoints of the API
server, or hit the stats url directly, e.g.
`curl $(minikube service -n akutan-dev akutan-api-http --url)/stats.txt`

### Deleting Your Cluster

To delete your entire Kubernetes cluster and start again, you can use:

```bash
$ minikube delete
```

This will delete your virtual machine. From there you can start
over from `minikube start` above.

You can instead use `kubectl delete ...` for a less radical approach.

**Note** If you `kubectl delete <APersistentVolume>` it removes the PersistentVolume
object from k8s, however the backing directory inside the minikube vm still exists
and is not cleared. So a subsequent `kubectl create <TheSamePersistentVolume>` ends
up with the volume having the contents that it had when it was deleted. Either use
`minikube delete` as above to start from scratch, or use `minikube ssh` to SSH into
the minikube VM and manually delete the directories. All the PVs created here are
stored in /mnt/akutan/. If you've deleted all the Akutan Persistent Volumes, you can do
`minikube ssh -- sudo rm -rf /mnt/akutan` to clean them all up.
