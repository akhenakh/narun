# Build narun-gw for different architectures
GOOS=linux GOARCH=amd64  CGO_ENABLED=1  CC="zig cc -target x86_64-linux-musl" go build -o bin/narun-gw-amd64 --ldflags '-linkmode=external -extldflags=-static'  ./cmd/narun-gw
GOOS=linux GOARCH=riscv64  CGO_ENABLED=1  CC="zig cc -target riscv64-linux-musl" go build -o bin/narun-gw-risc-v --ldflags '-linkmode=external -extldflags=-static' ./cmd/narun-gw
GOOS=linux GOARCH=arm64  CGO_ENABLED=1  CC="zig cc -target aarch64-linux-musl" go build -o bin/narun-gw-aarch64 --ldflags '-linkmode=external -extldflags=-static' ./cmd/narun-gw

# Build narun
GOOS=linux GOARCH=amd64  CGO_ENABLED=1  CC="zig cc -target x86_64-linux-musl" go build -o bin/narun-amd64 --ldflags '-linkmode=external -extldflags=-static'  ./cmd/narun
GOOS=linux GOARCH=riscv64  CGO_ENABLED=1  CC="zig cc -target riscv64-linux-musl" go build -o bin/narun-risc-v --ldflags '-linkmode=external -extldflags=-static' ./cmd/narun
GOOS=linux GOARCH=arm64  CGO_ENABLED=1  CC="zig cc -target aarch64-linux-musl" go build -o bin/narun-aarch64 --ldflags '-linkmode=external -extldflags=-static' ./cmd/narun

# Build node-runner
GOOS=linux GOARCH=amd64  CGO_ENABLED=1  CC="zig cc -target x86_64-linux-musl" go build -o bin/node-runner-amd64 --ldflags '-linkmode=external -extldflags=-static'  ./cmd/node-runner
GOOS=linux GOARCH=riscv64  CGO_ENABLED=1  CC="zig cc -target riscv64-linux-musl" go build -o bin/node-runner-risc-v --ldflags '-linkmode=external -extldflags=-static' ./cmd/node-runner
GOOS=linux GOARCH=arm64  CGO_ENABLED=1  CC="zig cc -target aarch64-linux-musl" go build -o bin/node-runner-aarch64 --ldflags '-linkmode=external -extldflags=-static' ./cmd/node-runner
