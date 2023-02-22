module github.com/DataDog/nola/virtual/benchmarks

go 1.19

replace github.com/richardartoul/nola => ../../

replace github.com/richardartoul/nola/virtual/registry/fdbregistry => ../../virtual/registry/fdbregistry

require (
	github.com/DataDog/sketches-go v1.4.1
	github.com/richardartoul/nola v0.0.0-00010101000000-000000000000
	github.com/richardartoul/nola/virtual/registry/fdbregistry v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.8.1
)

require (
	github.com/Workiva/go-datastructures v1.0.53 // indirect
	github.com/apple/foundationdb/bindings/go v0.0.0-20230221214914-bb4fb3d81d44 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/tetratelabs/wazero v1.0.0-pre.5 // indirect
	github.com/wapc/wapc-go v0.5.6 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.0.0-20221010170243-090e33056c14 // indirect
	golang.org/x/xerrors v0.0.0-20220411194840-2f41105eb62f // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
