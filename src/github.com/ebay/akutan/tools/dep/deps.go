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

package main

var deps = []Dep{
	{
		// Temporarily use fork that has Any() fix until it gets
		// consumed by the upstream project
		repo:    "https://github.com/superfell/goparsify.git",
		dir:     "src/vendor/github.com/vektah/goparsify",
		version: "913ce144773677c27a0b938ca1b66b7a65a6270a",
	},
	{
		repo:    "https://github.com/mattn/go-runewidth.git",
		dir:     "src/vendor/github.com/mattn/go-runewidth",
		version: "ce7b0b5c7b45a81508558cd1dba6bb1e4ddb51bb",
	},
	{
		repo:    "https://github.com/cheggaaa/pb.git",
		dir:     "src/vendor/github.com/cheggaaa/pb",
		version: "f907f6f5dd81f77c2bbc1cde92e4c5a04720cb11",
	},

	{
		repo:    "https://github.com/julienschmidt/httprouter.git",
		dir:     "src/vendor/github.com/julienschmidt/httprouter",
		version: "adbc77eec0d91467376ca515bc3a14b8434d0f18",
	},

	{
		repo:    "https://github.com/gogo/protobuf",
		dir:     "src/vendor/github.com/gogo/protobuf",
		version: "v1.1.1",
	},

	{
		repo:    "https://github.com/golang/net.git",
		dir:     "src/vendor/golang.org/x/net",
		version: "a463015", // latest release-branch.go1.11 as of 2019-01-01
	},
	{
		repo:    "https://github.com/golang/text.git",
		dir:     "src/vendor/golang.org/x/text",
		version: "cb67308", // latest release-branch.go1.11 as of 2019-01-01
	},
	{
		repo:    "https://github.com/grpc/grpc-go.git",
		dir:     "src/vendor/google.golang.org/grpc",
		version: "v1.19.0",
	},
	{
		repo:    "https://github.com/golang/protobuf",
		dir:     "src/vendor/github.com/golang/protobuf",
		version: "v1.2.0",
	},
	{
		repo:    "https://github.com/google/go-genproto",
		dir:     "src/vendor/google.golang.org/genproto",
		version: "11092d34479b07829b72e10713b159248caf5dad",
	},
	{
		repo:    "https://github.com/rcrowley/go-metrics",
		dir:     "src/vendor/github.com/rcrowley/go-metrics",
		version: "e2704e165165ec55d062f5919b4b29494e9fa790",
	},
	{
		repo:    "https://github.com/davecgh/go-spew",
		dir:     "src/vendor/github.com/davecgh/go-spew",
		version: "v1.1.1",
	},
	{
		repo:    "https://github.com/eapache/go-resiliency",
		dir:     "src/vendor/github.com/eapache/go-resiliency",
		version: "ea41b0fad31007accc7f806884dcdf3da98b79ce",
	},
	{
		repo:    "https://github.com/eapache/go-xerial-snappy",
		dir:     "src/vendor/github.com/eapache/go-xerial-snappy",
		version: "bb955e01b9346ac19dc29eb16586c90ded99a98c",
	},
	{
		repo:    "https://github.com/eapache/queue",
		dir:     "src/vendor/github.com/eapache/queue",
		version: "093482f3f8ce946c05bcba64badd2c82369e084d",
	},
	{
		repo:    "https://github.com/pierrec/lz4",
		dir:     "src/vendor/github.com/pierrec/lz4",
		version: "ed8d4cc3b461464e69798080a0092bd028910298",
	},
	{
		repo:    "https://github.com/golang/snappy",
		dir:     "src/vendor/github.com/golang/snappy",
		version: "553a641470496b2327abcac10b36396bd98e45c9",
	},
	{
		repo:    "https://github.com/pierrec/xxHash",
		dir:     "src/vendor/github.com/pierrec/xxHash",
		version: "a0006b13c722f7f12368c00a3d3c2ae8a999a0c6",
	},
	{
		repo:    "https://github.com/Shopify/sarama.git",
		dir:     "src/vendor/gopkg.in/Shopify/sarama.v1",
		version: "3c763ff04e6daa57d4a4614e5bcd908f2527c989",
	},
	{
		repo:    "https://github.com/pkg/errors.git",
		dir:     "src/vendor/github.com/pkg/errors",
		version: "816c9085562cd7ee03e7f8188a1cfd942858cded",
	},
	{
		repo:    "https://github.com/golang/sys.git",
		dir:     "src/vendor/golang.org/x/sys",
		version: "98c5dad", // latest release-branch.go1.11 as of 2019-01-01
	},
	//	{
	//		repo:    "https://github.com/cheekybits/genny.git",
	//		dir:     "src/vendor/github.com/cheekybits/genny",
	//		version: "9127e812e1e9e501ce899a18121d316ecb52e4ba",
	//	},
	//
	// Using this fork of genny instead as it appears to be maintained
	// and the origin one isn't
	{
		repo:    "https://github.com/mauricelam/genny.git",
		dir:     "src/vendor/github.com/mauricelam/genny",
		version: "11e1a0b9d8936dda6aa9b8f4bfeae2a6715ea858",
	},

	{
		repo: "https://github.com/golang/tools",
		dir:  "src/vendor/golang.org/x/tools",
		// I think we would rather stay on the release-branch.go.1.11 branch,
		// but staticcheck requires packages.Visit, which wasn't added until
		// later (0aa4b883).
		version: "ca9055ed", // latest master as of 2019-01-01
	},
	{
		repo:    "https://github.com/stretchr/testify.git",
		dir:     "src/vendor/github.com/stretchr/testify",
		version: "v1.2.2",
	},

	{
		repo:    "https://github.com/tecbot/gorocksdb",
		dir:     "src/vendor/github.com/tecbot/gorocksdb",
		version: "4ab001a8f8655a0c190bd33d8fbb3fc321b6e346",
	},

	{
		repo: "https://github.com/sirupsen/logrus.git",
		dir:  "src/vendor/github.com/sirupsen/logrus",
		// Showing the file and line number was added just after v1.1.1.
		version: "566a5f6",
	},
	{
		repo:    "https://github.com/golang/crypto",
		dir:     "src/vendor/golang.org/x/crypto",
		version: "56440b8", // latest release-branch.go1.11 as of 2019-01-01
	},

	// goreman
	{
		repo:    "https://github.com/mattn/goreman",
		dir:     "src/vendor/github.com/mattn/goreman",
		version: "abf5a3ff7c598bb9d775d98efcc6677be0759ab7",
	},
	{
		repo:    "https://github.com/joho/godotenv",
		dir:     "src/vendor/github.com/joho/godotenv",
		version: "1709ab122c988931ad53508747b3c061400c2984",
	},
	{
		repo:    "https://gopkg.in/yaml.v2",
		dir:     "src/vendor/gopkg.in/yaml.v2",
		version: "5420a8b6744d3b0345ab293f6fcba19c978f1183",
	},
	{
		repo:    "https://github.com/daviddengcn/go-colortext",
		dir:     "src/vendor/github.com/daviddengcn/go-colortext",
		version: "186a3d44e9200d7eb331356ca4864f52708e1399",
	},

	{
		repo:    "https://github.com/docopt/docopt.go",
		dir:     "src/vendor/github.com/docopt/docopt-go",
		version: "ee0de3bc6815ee19d4a46c7eb90f829db0e014b1",
	},

	// OpenTracing and Jaeger
	{
		repo:    "https://github.com/opentracing/opentracing-go",
		dir:     "src/vendor/github.com/opentracing/opentracing-go",
		version: "v1.0.2",
	},
	{
		repo: "https://github.com/jaegertracing/jaeger-client-go",
		dir:  "src/vendor/github.com/uber/jaeger-client-go",
		// This is a few commits past v2.15.0. That version was incompatible
		// with jaeger-lib v2.0.0.
		version: "6733ee4",
	},
	{
		repo:    "https://github.com/jaegertracing/jaeger-lib",
		dir:     "src/vendor/github.com/uber/jaeger-lib",
		version: "v2.0.0",
	},
	{
		repo:    "https://github.com/grpc-ecosystem/grpc-opentracing",
		dir:     "src/vendor/github.com/grpc-ecosystem/grpc-opentracing",
		version: "151e1e2",
	},

	// golint
	{
		repo:    "https://github.com/golang/lint.git",
		dir:     "src/vendor/golang.org/x/lint",
		version: "8f45f77", // latest master as of 2019-01-01
	},

	// staticcheck
	{
		repo:    "https://github.com/dominikh/go-tools.git",
		dir:     "src/vendor/honnef.co/go/tools",
		version: "2019.1.1",
	},
	{
		repo:    "https://github.com/BurntSushi/toml.git",
		dir:     "src/vendor/github.com/BurntSushi/toml",
		version: "v0.3.1",
	},
	// staticcheck also requires golang.org/x/tools

	// b-tree
	{
		repo:    "https://github.com/google/btree.git",
		dir:     "src/vendor/github.com/google/btree",
		version: "4030bb1",
	},

	// prometheus client
	{
		repo:    "https://github.com/prometheus/client_golang.git",
		dir:     "src/vendor/github.com/prometheus/client_golang",
		version: "v0.9.0",
	},
	{
		repo:    "https://github.com/beorn7/perks.git",
		dir:     "src/vendor/github.com/beorn7/perks",
		version: "3a771d992973f24aa725d07868b467d1ddfceafb",
	},
	{
		repo:    "https://github.com/prometheus/client_model.git",
		dir:     "src/vendor/github.com/prometheus/client_model",
		version: "5c3871d89910bfb32f5fcab2aa4b9ec68e65a99f",
	},
	{
		repo:    "https://github.com/prometheus/common.git",
		dir:     "src/vendor/github.com/prometheus/common",
		version: "c7de2306084e37d54b8be01f3541a8464345e9a5",
	},
	{
		repo:    "https://github.com/prometheus/procfs.git",
		dir:     "src/vendor/github.com/prometheus/procfs",
		version: "185b4288413d2a0dd0806f78c90dde719829e5ae",
	},
	{
		repo:    "https://github.com/matttproud/golang_protobuf_extensions.git",
		dir:     "src/vendor/github.com/matttproud/golang_protobuf_extensions",
		version: "v1.0.1",
	},
	{
		repo:    "https://github.com/grpc-ecosystem/go-grpc-prometheus.git",
		dir:     "src/vendor/github.com/grpc-ecosystem/go-grpc-prometheus",
		version: "v1.2.0",
	},

	// Using Eric's client because it is orders of magnitude simpler than the
	// officially supported deathstar.
	// https://kubernetes.io/docs/reference/using-api/client-libraries/
	{
		repo:    "https://github.com/ericchiang/k8s.git",
		dir:     "src/vendor/github.com/ericchiang/k8s",
		version: "v1.2.0",
	},

	{
		repo:    "https://github.com/cespare/xxhash.git",
		dir:     "src/vendor/github.com/cespare/xxhash",
		version: "v2.0.0",
	},
}
