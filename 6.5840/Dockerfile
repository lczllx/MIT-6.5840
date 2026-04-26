# 在「含 Makefile 与 src/ 的这一层」构建，例如：cd mit6.5840/6.5840 && docker build -t 6.5840-mit .
# 覆盖 CMD 示例：docker run --rm 6.5840-mit bash -lc "go test -v -count=1 -run '3A' -race ./raft1"

FROM golang:1.22-bookworm

WORKDIR /app
COPY src/ ./

ENV CGO_ENABLED=1

RUN go mod download

RUN go build -race -o main/mrsequential main/mrsequential.go && \
    go build -race -o main/mrcoordinator main/mrcoordinator.go && \
    go build -race -o main/mrworker main/mrworker.go && \
    (cd mrapps && go build -race -buildmode=plugin wc.go) && \
    (cd mrapps && go build -race -buildmode=plugin indexer.go) && \
    (cd mrapps && go build -race -buildmode=plugin mtiming.go) && \
    (cd mrapps && go build -race -buildmode=plugin rtiming.go) && \
    (cd mrapps && go build -race -buildmode=plugin jobcount.go) && \
    (cd mrapps && go build -race -buildmode=plugin early_exit.go) && \
    (cd mrapps && go build -race -buildmode=plugin crash.go) && \
    (cd mrapps && go build -race -buildmode=plugin nocrash.go) && \
    go build -race -o main/kvsrv1d main/kvsrv1d.go && \
    go build -race -o main/raft1d main/raft1d.go

# mr → kvsrv1 → lock → raft1(3A–3D)
CMD ["bash","-lc","set -e; go test -v -count=1 -race ./mr; go test -v -count=1 -race ./kvsrv1; go test -v -count=1 -race ./kvsrv1/lock; go test -v -count=1 -timeout=45m -race -run '3A|3B|3C|3D' ./raft1"]
