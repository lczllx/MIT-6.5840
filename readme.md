lab1
lcz@iv-yef3xahqtc5i3z5jzmr5:~/mit6.5840/6.5840/src$ make mr
go build -race -o main/mrsequential main/mrsequential.go
go build -race -o main/mrcoordinator main/mrcoordinator.go
go build -race -o main/mrworker main/mrworker.go&
(cd mrapps && go build -race -buildmode=plugin wc.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin indexer.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin mtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin rtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin jobcount.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin early_exit.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin crash.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin nocrash.go) || exit 1
cd mr; go test -v -race 
=== RUN   TestWc
--- PASS: TestWc (10.58s)
=== RUN   TestIndexer
--- PASS: TestIndexer (6.78s)
=== RUN   TestMapParallel
--- PASS: TestMapParallel (8.02s)
=== RUN   TestReduceParallel
--- PASS: TestReduceParallel (9.03s)
=== RUN   TestJobCount
--- PASS: TestJobCount (12.03s)
=== RUN   TestEarlyExit
--- PASS: TestEarlyExit (7.03s)
=== RUN   TestCrashWorker
2026/03/02 21:01:29 检测到 Map 任务 0 超时，重置为 Idle
2026/03/02 21:01:29 检测到 Map 任务 1 超时，重置为 Idle
2026/03/02 21:01:29 检测到 Map 任务 4 超时，重置为 Idle
2026/03/02 21:02:01 检测到 Reduce 任务 0 超时，重置为 Idle
2026/03/02 21:02:01 检测到 Reduce 任务 2 超时，重置为 Idle
2026/03/02 21:02:01 检测到 Reduce 任务 5 超时，重置为 Idle
2026/03/02 21:02:01 检测到 Reduce 任务 9 超时，重置为 Idle
2026/03/02 21:02:31 检测到 Reduce 任务 9 超时，重置为 Idle
--- PASS: TestCrashWorker (97.14s)
PASS
ok      6.5840/mr       151.614s