
GOOS=linux GOARCH=amd64  CGO_ENABLED=1  CC="zig cc -target x86_64-linux-musl" go build -o narun-gw-amd64 --ldflags '-linkmode=external -extldflags=-static'
GOOS=linux GOARCH=riscv64  CGO_ENABLED=1  CC="zig cc -target riscv64-linux-musl" go build -o narun-gw-risc-v --ldflags '-linkmode=external -extldflags=-static'
GOOS=linux GOARCH=arm64  CGO_ENABLED=1  CC="zig cc -target aarch64-linux-musl" go build -o narun-gw-aarch64 --ldflags '-linkmode=external -extldflags=-static'
