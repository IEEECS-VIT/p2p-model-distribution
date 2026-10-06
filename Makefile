.PHONY: setup build test proto

setup:
	git config core.hooksPath .githooks
	chmod -R +x .githooks/
	@echo "Git hooks configured!"

build:
	go build -o bin/p2pmd ./cmd/p2pmd

test:
	go vet ./...
	go test -race -count=1 ./...

proto:
	protoc --go_out=. --go_opt=module=github.com/IEEECS-VIT/p2p-model-distribution proto/p2p.proto
