module github.com/richardartoul/nola

go 1.19

require (
	github.com/DataDog/sketches-go v1.4.1
	github.com/buger/jsonparser v1.1.1
	github.com/dgraph-io/ristretto v0.1.1
	github.com/google/btree v1.1.2
	github.com/stretchr/testify v1.8.1
	github.com/tetratelabs/wazero v1.0.1
	github.com/wapc/wapc-go v0.5.7
	github.com/wapc/wapc-guest-tinygo v0.3.3
	github.com/wasmerio/wasmer-go v1.0.4
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29
	golang.org/x/sync v0.1.0
)

require (
	github.com/Workiva/go-datastructures v1.0.53 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// wapc-go only works with pre.6 because pre.9 changes the API slightly. Once wapc-go
// is released to work with pre.9 we can remove this and upgrade.
replace github.com/tetratelabs/wazero => github.com/tetratelabs/wazero v1.0.0-pre.6
