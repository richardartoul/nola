module github.com/richardartoul/nola/cmd/app

go 1.19

replace github.com/richardartoul/nola => ../../

require (
	github.com/DataDog/sketches-go v1.4.1
	github.com/google/uuid v1.3.0
	github.com/richardartoul/nola v0.0.0-00010101000000-000000000000
	github.com/richardartoul/nola/virtual/registry/fdbregistry v0.0.0-20230316040541-d4eae35f2278
	github.com/stretchr/testify v1.8.1
)

require (
	github.com/Workiva/go-datastructures v1.0.53 // indirect
	github.com/apple/foundationdb/bindings/go v0.0.0-20230901003605-bfb1c51299a5 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/tetratelabs/wazero v1.0.1 // indirect
	github.com/wapc/wapc-go v0.5.7 // indirect
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
