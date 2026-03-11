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

- lcz@iv-yef3xahqtc5i3z5jzmr5:~/6.5840/src$ make kvsrv1
- go build -race -o main/kvsrv1d main/kvsrv1d.go
- cd kvsrv1 && go test -v -race  
- === RUN   TestReliablePut
- One client and reliable Put (reliable network)...
-  ... Passed --  time  0.0s #peers 1 #RPCs     5 #Ops    5
- --- PASS: TestReliablePut (0.12s)
- === RUN   TestPutConcurrentReliable
- Test: many clients racing to put values to the same key (reliable network)...
-  ... Passed --  time  1.6s #peers 1 #RPCs  2393 #Ops 4786
- --- PASS: TestPutConcurrentReliable (1.85s)
- === RUN   TestMemPutManyClientsReliable
- Test: memory use many put clients (reliable network)...
-   ... Passed --  time 28.2s #peers 1 #RPCs 20000 #Ops 20000
- --- PASS: TestMemPutManyClientsReliable (53.10s)
- === RUN   TestUnreliableNet
- One client (unreliable network)...
-  ... Passed --  time  4.0s #peers 1 #RPCs   248 #Ops  416
- --- PASS: TestUnreliableNet (4.12s)
- PASS
- ok      6.5840/kvsrv1   60.218s

- lcz@iv-yef3xahqtc5i3z5jzmr5:~/6.5840/src$ make lock1
- go build -race -o main/kvsrv1d main/kvsrv1d.go
- cd kvsrv1/lock; go test -v -race 
- === RUN   TestReliableBasic
- Test: a single Acquire and Release (reliable network)...
-   ... Passed --  time  0.0s #peers 1 #RPCs     4 #Ops    4
- --- PASS: TestReliableBasic (0.12s)
- === RUN   TestReliableNested
- Test: one client, two locks (reliable network)...
-   ... Passed --  time  0.0s #peers 1 #RPCs    20 #Ops   20
- --- PASS: TestReliableNested (0.14s)
- === RUN   TestOneClientReliable
- Test: 1 lock clients (reliable network)...
-   ... Passed --  time  2.0s #peers 1 #RPCs   716 #Ops  716
- --- PASS: TestOneClientReliable (2.12s)
- === RUN   TestManyClientsReliable
- Test: 10 lock clients (reliable network)...
-   ... Passed --  time  2.2s #peers 1 #RPCs  3375 #Ops 3375
- --- PASS: TestManyClientsReliable (2.34s)
- === RUN   TestOneClientUnreliable
- Test: 1 lock clients (unreliable network)...
-   ... Passed --  time  2.1s #peers 1 #RPCs   133 #Ops  104
- --- PASS: TestOneClientUnreliable (2.21s)
- === RUN   TestManyClientsUnreliable
- Test: 10 lock clients (unreliable network)...
-   ... Passed --  time  3.1s #peers 1 #RPCs  1425 #Ops 1133
- --- PASS: TestManyClientsUnreliable (3.24s)
- PASS
- ok      6.5840/kvsrv1/lock      11.178s