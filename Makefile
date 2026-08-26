.PHONY: test vet fmt ci clean

test: ## run the full suite with the race detector
	go test -race ./...

vet: ## go vet, staticcheck, and a gofmt diff check
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest -checks=all ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:" && gofmt -l . && exit 1)

fmt: ## rewrite formatting in place
	gofmt -w .

ci: vet test

clean: ## remove build artifacts
	go clean -testcache
