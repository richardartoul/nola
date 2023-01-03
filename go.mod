module github.com/richardartoul/nola

go 1.19

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20220521054011-a88e049b28d8
	github.com/google/btree v1.1.2
	github.com/google/uuid v1.3.0
	github.com/stretchr/testify v1.7.1
	github.com/tetratelabs/wazero v1.0.0-pre.5
	github.com/wapc/wapc-go v0.5.6
	github.com/wapc/wapc-guest-tinygo v0.3.3
	github.com/wasmerio/wasmer-go v1.0.4
)

require (
	github.com/Workiva/go-datastructures v1.0.53 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
	golang.org/x/xerrors v0.0.0-20220411194840-2f41105eb62f // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)

// wapc-go only works with pre.4 because pre.5 changes the API slightly. Once wapc-go
// is released to work with pre.5 we can remove this and upgrade.
replace github.com/tetratelabs/wazero => github.com/tetratelabs/wazero v1.0.0-pre.4
