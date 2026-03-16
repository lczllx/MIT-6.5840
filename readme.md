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

lcz@iv-yef3xahqtc5i3z5jzmr5:~/mit6.5840/6.5840/src$ make RUN="-run 3A" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3A 
=== RUN   TestInitialElection3A
Test (3A): initial election (reliable network)...
  ... Passed --  time  3.0s #peers 3 #RPCs   192 #Ops    0
--- PASS: TestInitialElection3A (3.47s)
=== RUN   TestReElection3A
Test (3A): election after network failure (reliable network)...
  ... Passed --  time  4.6s #peers 3 #RPCs   390 #Ops    0
--- PASS: TestReElection3A (5.12s)
=== RUN   TestManyElections3A
Test (3A): multiple elections (reliable network)...
  ... Passed --  time  5.6s #peers 7 #RPCs  1680 #Ops    0
--- PASS: TestManyElections3A (6.59s)
PASS
ok      6.5840/raft1    16.203s

lcz@iv-yef3xahqtc5i3z5jzmr5:~/mit6.5840/6.5840/src$ make RUN="-run 3B" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3B 
=== RUN   TestBasicAgree3B
Test (3B): basic agreement (reliable network)...
  ... Passed --  time  0.4s #peers 3 #RPCs    14 #Ops    3
--- PASS: TestBasicAgree3B (0.71s)
=== RUN   TestRPCBytes3B
Test (3B): RPC byte count (reliable network)...
  ... Passed --  time  1.8s #peers 3 #RPCs    58 #Ops   11
--- PASS: TestRPCBytes3B (2.14s)
=== RUN   TestFollowerFailure3B
Test (3B): test progressive failure of followers (reliable network)...
  ... Passed --  time  4.3s #peers 3 #RPCs   188 #Ops    3
--- PASS: TestFollowerFailure3B (4.67s)
=== RUN   TestLeaderFailure3B
Test (3B): test failure of leaders (reliable network)...
  ... Passed --  time  4.7s #peers 3 #RPCs   294 #Ops    3
--- PASS: TestLeaderFailure3B (5.03s)
=== RUN   TestFailAgree3B
Test (3B): agreement after follower reconnects (reliable network)...
  ... Passed --  time  3.9s #peers 3 #RPCs   134 #Ops    7
--- PASS: TestFailAgree3B (4.37s)
=== RUN   TestFailNoAgree3B
Test (3B): no agreement if too many followers disconnect (reliable network)...
  ... Passed --  time  3.3s #peers 5 #RPCs   316 #Ops    2
--- PASS: TestFailNoAgree3B (3.81s)
=== RUN   TestConcurrentStarts3B
Test (3B): concurrent Start()s (reliable network)...
  ... Passed --  time  0.6s #peers 3 #RPCs    24 #Ops    0
--- PASS: TestConcurrentStarts3B (1.07s)
=== RUN   TestRejoin3B
Test (3B): rejoin of partitioned leader (reliable network)...
  ... Passed --  time  5.7s #peers 3 #RPCs   282 #Ops    4
--- PASS: TestRejoin3B (6.05s)
=== RUN   TestBackup3B
Test (3B): leader backs up quickly over incorrect follower logs (reliable network)...
  ... Passed --  time 19.1s #peers 5 #RPCs  2568 #Ops  102
--- PASS: TestBackup3B (19.68s)
=== RUN   TestCount3B
Test (3B): RPC counts aren't too high (reliable network)...
  ... Passed --  time  2.2s #peers 3 #RPCs    72 #Ops    0
--- PASS: TestCount3B (2.71s)
PASS
ok      6.5840/raft1    51.273s