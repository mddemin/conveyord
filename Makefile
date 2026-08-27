.PHONY: test vet fmt ci clean proto proto-breaking
PROTO_FILES := $(shell find proto -name '*.proto')

proto: ## regenerate gRPC code from the schemas
	go tool buf format -w proto
	go tool buf lint
	go tool buf generate

proto-breaking: ## fail if the schema broke wire compatibility vs main
	go tool buf breaking proto --against '.git#branch=main,subdir=proto'

vet: ## go vet, staticcheck, and a gofmt diff check (generated code exempt)
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest -checks=all $$(go list ./... | grep -v '/gen/')
	@test -z "$$(gofmt -l . | grep -v '^gen/')" || (echo "gofmt needed:" && gofmt -l . && exit 1)

test: ## run the full suite with the race detector
	go test -race ./...

fmt: ## rewrite formatting in place
	gofmt -w .

ci: vet test

clean: ## remove build artifacts
	go clean -testcache
