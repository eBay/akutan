SHELL=/bin/bash
export GOPATH=$(shell pwd)
ROOT=$(shell pwd)
PACKAGE=github.com/ebay/akutan
# gorocksdb build settings
export CGO_LDFLAGS=-g -O2 -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd

.PHONY: clean get test vet cover run dist protoc no_std_log bench fmt 

all: get build test vet

clean:
	rm -rf src/vendor
	rm -rf bin
	rm -rf pkg
	rm -rf dist
	find src/${PACKAGE} -name "*.pb.go" -delete
	rm -f src/github.com/ebay/akutan/util/cmp/gen_cmp.go

# Set up git hooks. If you want to opt out of the hooks, set this config value
# to blank or some other directory.
hooks:
	git config core.hooksPath >/dev/null || git config core.hooksPath githooks

get: get_clone get_install

get_clone:
	mkdir -p src/vendor/github.com
	go run src/github.com/ebay/akutan/tools/dep/*.go

get_install:
	go install vendor/github.com/gogo/protobuf/protoc-gen-gogoslick
	go install vendor/github.com/mauricelam/genny
	go install vendor/github.com/mattn/goreman
	go install vendor/golang.org/x/lint/golint
	go install vendor/golang.org/x/tools/cmd/goimports
	go install vendor/honnef.co/go/tools/cmd/staticcheck

# generate .go code for .proto input using the gogoslick plugin
# https://github.com/gogo/protobuf#more-speed-and-more-generated-code
# $1 = output directory
# $2 = input file directory (may or may not be the same as output directory)
# $3 = input file name (no directory)
define protoc_go
	PATH=${ROOT}/bin:${PATH} protoc --gogoslick_out=plugins=grpc:$(strip $1) -Isrc:src/vendor:$(strip $2) $(strip $3)
	( \
		echo; \
		echo '//lint:file-ignore ST1006 staticcheck disapproves of generated code'; \
		echo '//lint:file-ignore ST1016 staticcheck disapproves of generated code'; \
	) >> $(strip $1)/$(basename $(strip $3)).pb.go
endef

src/github.com/ebay/akutan/rpc/views.pb.go: src/github.com/ebay/akutan/rpc/views.proto src/github.com/ebay/akutan/tools/dep/deps.go
	$(call protoc_go, src/github.com/ebay/akutan/rpc, src/github.com/ebay/akutan/rpc, views.proto)

src/github.com/ebay/akutan/logentry/commands.pb.go: src/github.com/ebay/akutan/logentry/commands.proto src/github.com/ebay/akutan/tools/dep/deps.go
	$(call protoc_go, src/github.com/ebay/akutan/logentry, src/github.com/ebay/akutan/logentry, commands.proto)

src/github.com/ebay/akutan/diskview/meta.pb.go: src/github.com/ebay/akutan/diskview/meta.proto src/github.com/ebay/akutan/tools/dep/deps.go
	$(call protoc_go, src/github.com/ebay/akutan/diskview, src/github.com/ebay/akutan/diskview, meta.proto)

src/github.com/ebay/akutan/api/akutan_api.pb.go: proto/api/akutan_api.proto src/github.com/ebay/akutan/tools/dep/deps.go
	$(call protoc_go, src/github.com/ebay/akutan/api, proto/api, akutan_api.proto)

src/github.com/ebay/akutan/logspec/log.pb.go: proto/logspec/log.proto src/github.com/ebay/akutan/tools/dep/deps.go
	$(call protoc_go, src/github.com/ebay/akutan/logspec, proto/logspec, log.proto)

src/github.com/ebay/akutan/tools/grpcbench/bench.pb.go: src/github.com/ebay/akutan/tools/grpcbench/bench.proto src/github.com/ebay/akutan/tools/dep/deps.go
	$(call protoc_go, src/github.com/ebay/akutan/tools/grpcbench, src/github.com/ebay/akutan/tools/grpcbench, bench.proto)

generate: protoc
	PATH=${PATH}:${ROOT}/bin go generate ${PACKAGE}/...

src/github.com/ebay/akutan/util/cmp/gen_cmp_test.go: src/github.com/ebay/akutan/util/cmp/cmp_test.template Makefile
	cat src/github.com/ebay/akutan/util/cmp/cmp_test.template | bin/genny gen "Scalar=int64,uint64,int32,uint32,int,string" > src/github.com/ebay/akutan/util/cmp/gen_cmp_test.go

src/github.com/ebay/akutan/util/cmp/gen_cmp.go: src/github.com/ebay/akutan/util/cmp/cmp.template Makefile
	cat src/github.com/ebay/akutan/util/cmp/cmp.template | bin/genny gen "Scalar=int64,uint64,int32,uint32,int,string" > src/github.com/ebay/akutan/util/cmp/gen_cmp.go

protoc: \
	src/github.com/ebay/akutan/rpc/views.pb.go \
	src/github.com/ebay/akutan/diskview/meta.pb.go \
	src/github.com/ebay/akutan/api/akutan_api.pb.go \
	src/github.com/ebay/akutan/logspec/log.pb.go \
	src/github.com/ebay/akutan/tools/grpcbench/bench.pb.go \
	src/github.com/ebay/akutan/logentry/commands.pb.go

build: generate src/github.com/ebay/akutan/util/cmp/gen_cmp.go src/github.com/ebay/akutan/util/cmp/gen_cmp_test.go
	go install ${PACKAGE}/...

lint:
	@FAIL=0; \
	for pkg in $$(go list ${PACKAGE}/...); do \
		if ! bin/golint --set_exit_status $$pkg; then \
			FAIL=1; \
		fi \
	done; \
	exit $$FAIL

staticcheck:
	bin/staticcheck -checks all ${PACKAGE}/...

vet: generate no_std_log lint staticcheck fmtcheck
	go vet ${PACKAGE}/...

no_std_log:
	@go list -f "{{.ImportPath}} {{range .Imports}} {{.}} {{end}}" ${PACKAGE}/... | grep ' log ' \
	| awk '{ if(err==0) {print "The following package(s) use the standard library log package, but should be using logrus instead";} print $$1; err=1 } END {exit err}'

test:
	go test -timeout 30s ${PACKAGE}/...

cover:
	go test -coverprofile=coverage.out -covermode=count ${PACKAGE}/...
	go tool cover -html=coverage.out

bench:
	go test -bench . ${PACKAGE}/...

# Reformats all the Go code. We used to use `go fmt` here, but goimports does
# all that and a little more.
fmt:
	bin/goimports -w src/github.com/ebay/akutan

# Verifies that all the Go code is well-formatted, without changing any of it.
#
# The ! and grep are to get the right exit status: any output from goimports
# should fail this target.
fmtcheck:
	@echo bin/goimports -d src/github.com/ebay/akutan
	@! bin/goimports -d src/github.com/ebay/akutan | grep '.\?'

.PHONY: local/generated/Procfile
local/generated/Procfile:
	bin/gen-local -cfg local/config.json -out local/generated

run: local/generated/Procfile
	CLICOLOR_FORCE=1 bin/goreman -f local/generated/Procfile start

# uses dist/ as the docker context
# $1 = component name (Dockerfile must exist at docker/${1}/Dockerfile)
define docker_build
	docker build --tag akutan-$(strip $1):latest --file docker/$(strip $1)/Dockerfile dist/
endef

docker-build-akutan-builder:
	docker build --tag=akutan-builder:latest docker/build
docker-build-api: dist
	$(call docker_build, api)
docker-build-diskview: dist
	$(call docker_build, diskview)
docker-build-kafka: dist
	$(call docker_build, kafka)
docker-build-txview: dist
	$(call docker_build, txview)

# Run the latest builder image locally.
#
# The docker.sock volume enables docker in the container to interact with docker on
# the host.
#
# The pwd volume enables development of docker images by mounting the host source
# tree into /home/builder/akutan
docker-run-akutan-builder:
	docker run --interactive --tty --rm \
		--hostname akutan-builder \
		--volume /var/run/docker.sock:/var/run/docker.sock \
		--volume "${PWD}:/home/builder/akutan" \
		akutan-builder:latest /bin/bash

# Create the Akutan builder docker image with the Docker daemon of Minukube. This
# builder image will then be used to build the core Akutan images.
docker-build-akutan-builder-in-minikube:
	eval $$(minikube docker-env || echo "exit 1"); \
	docker build --tag=akutan-builder:latest docker/build

# Build the core Akutan images using the Akutan builder image running within
# Minikube's Docker. These images will then be accessible from Minikube's
# Kubernetes.
#
# Note that these volume mounts are mounting volumes from Minikube's VirtualBox
# VM into the container. The reason ${PWD} is meaningful is because VirtualBox
# mounts /Users/ from the Mac host into the VM. Other directories, like
# /var/run, are local to the VM.
docker-build-akutan-service-in-minikube:
	eval $$(minikube docker-env || echo "exit 1"); \
	docker run --interactive --tty --rm \
		--hostname akutan-builder \
		--volume /var/run/docker.sock:/var/run/docker.sock \
		--volume /home/docker/.docker:/home/builder/.docker \
		--volume "${PWD}:/home/builder/akutan" \
		--entrypoint /bin/bash \
		akutan-builder:latest \
		-c "cd /home/builder/akutan && make docker-build-{api,diskview,txview,kafka}"; \
	docker tag akutan-api:latest akutan-api:local; \
	docker tag akutan-diskview:latest akutan-diskview:local; \
	docker tag akutan-txview:latest akutan-txview:local; \
	docker tag akutan-kafka:latest akutan-kafka:local

dist: build
	mkdir -p dist/docker
	cp bin/* dist/
	cp docker/kafka/{init.sh,*.properties} dist/docker
	tar --exclude dist/akutan.tar.lz4 -c dist/ |lz4 -f - dist/akutan.tar.lz4

failed:
	g++ --version
	gcc --version
	go version
	protoc --version
	find src -type d -name .git -print -exec git --git-dir={} log -n 1 --pretty=format:'%H %cD' \;
	test -f /usr/local/lib/librocksdb.a && strings /usr/local/lib/librocksdb.a |grep -E 'rocksdb_build_git_(date|sha):'
	ls -l /var/run/docker.sock
	id
	docker --version
