module github.com/richardartoul/nola

go 1.19

require (
	github.com/google/uuid v1.3.0
	github.com/stretchr/testify v1.7.0
	github.com/tetratelabs/wazero v1.0.0-pre.5
	github.com/wapc/wapc-go v0.5.6
	github.com/wapc/wapc-guest-tinygo v0.3.3
	github.com/wasmerio/wasmer-go v1.0.4
)

require (
	github.com/Workiva/go-datastructures v1.0.53 // indirect
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

// wapc-go only works with pre.4 because pre.5 changes the API slightly. Once wapc-go
// is released to work with pre.5 we can remove this and upgrade.
replace github.com/tetratelabs/wazero => github.com/tetratelabs/wazero v1.0.0-pre.4
