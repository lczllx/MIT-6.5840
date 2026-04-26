# 6.5840 实验测试记录

实验代码与 `make` 工作目录均为 **`6.5840/src/`**（本仓库中对应路径：`mit6.5840/6.5840/src`）。以下均为带 **`-race`** 的 `go test` 输出留档。

<a id="summary-sec"></a>

## 速览

| 实验 | 命令 | 总耗时（摘录） |
|------|------|----------------|
| MapReduce (Lab2) | `make mr` | `ok 6.5840/mr` ~151.6s |
| kvsrv1 | `make kvsrv1` | `ok 6.5840/kvsrv1` ~60.2s |
| lock | `make lock1` | `ok .../kvsrv1/lock` ~11.2s |
| Raft 3A | `make RUN="-run 3A" raft1` | `ok 6.5840/raft1` ~16.2s |
| Raft 3B | `make RUN="-run 3B" raft1` | ~51.3s |
| Raft 3C | `make RUN="-run 3C" raft1` | ~160.7s |
| Raft 3D | `make RUN="-run 3D" raft1` | ~172.2s |

## 目录

- [速览](#summary-sec)
- [相关博客](#blog-sec)
- [1. MapReduce](#mapreduce-sec)
- [2. kvsrv1](#kvsrv1-sec)
- [3. lock](#lock-sec)
- [4. Raft (3A–3D)](#raft-sec)
- [5. CI 与 Docker](#cicd-sec)

<a id="blog-sec"></a>

## 相关博客

- Lab1 博客：<https://blog.csdn.net/2401_87734250/article/details/158584577>
- 3A 博客：<https://blog.csdn.net/2401_87734250/article/details/159122467>

---

<a id="mapreduce-sec"></a>

## 1. MapReduce

_命令：`make mr`_

```bash
cd mit6.5840/6.5840/src   # 按你本机实际路径
make mr
```

```text
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
```

<a id="kvsrv1-sec"></a>

## 2. kvsrv1

_命令：`make kvsrv1`_

```bash
make kvsrv1
```

```text
lcz@iv-yef3xahqtc5i3z5jzmr5:~/6.5840/src$ make kvsrv1
go build -race -o main/kvsrv1d main/kvsrv1d.go
cd kvsrv1 && go test -v -race  
=== RUN   TestReliablePut
One client and reliable Put (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs     5 #Ops    5
--- PASS: TestReliablePut (0.12s)
=== RUN   TestPutConcurrentReliable
Test: many clients racing to put values to the same key (reliable network)...
  ... Passed --  time  1.6s #peers 1 #RPCs  2393 #Ops 4786
--- PASS: TestPutConcurrentReliable (1.85s)
=== RUN   TestMemPutManyClientsReliable
Test: memory use many put clients (reliable network)...
  ... Passed --  time 28.2s #peers 1 #RPCs 20000 #Ops 20000
--- PASS: TestMemPutManyClientsReliable (53.10s)
=== RUN   TestUnreliableNet
One client (unreliable network)...
  ... Passed --  time  4.0s #peers 1 #RPCs   248 #Ops  416
--- PASS: TestUnreliableNet (4.12s)
PASS
ok      6.5840/kvsrv1   60.218s
```

<a id="lock-sec"></a>

## 3. lock

_命令：`make lock1`_

```bash
make lock1
```

```text
lcz@iv-yef3xahqtc5i3z5jzmr5:~/6.5840/src$ make lock1
go build -race -o main/kvsrv1d main/kvsrv1d.go
cd kvsrv1/lock; go test -v -race 
=== RUN   TestReliableBasic
Test: a single Acquire and Release (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs     4 #Ops    4
--- PASS: TestReliableBasic (0.12s)
=== RUN   TestReliableNested
Test: one client, two locks (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs    20 #Ops   20
--- PASS: TestReliableNested (0.14s)
=== RUN   TestOneClientReliable
Test: 1 lock clients (reliable network)...
  ... Passed --  time  2.0s #peers 1 #RPCs   716 #Ops  716
--- PASS: TestOneClientReliable (2.12s)
=== RUN   TestManyClientsReliable
Test: 10 lock clients (reliable network)...
  ... Passed --  time  2.2s #peers 1 #RPCs  3375 #Ops 3375
--- PASS: TestManyClientsReliable (2.34s)
=== RUN   TestOneClientUnreliable
Test: 1 lock clients (unreliable network)...
  ... Passed --  time  2.1s #peers 1 #RPCs   133 #Ops  104
--- PASS: TestOneClientUnreliable (2.21s)
=== RUN   TestManyClientsUnreliable
Test: 10 lock clients (unreliable network)...
  ... Passed --  time  3.1s #peers 1 #RPCs  1425 #Ops 1133
--- PASS: TestManyClientsUnreliable (3.24s)
PASS
ok      6.5840/kvsrv1/lock      11.178s
```

<a id="raft-sec"></a>

## 4. Raft (3A–3D)

统一在 `6.5840/src` 下执行，例如 `make RUN="-run 3A" raft1`；以下为分阶段完整输出。

### 3A

```bash
make RUN="-run 3A" raft1
```

```text
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
```

### 3B

```bash
make RUN="-run 3B" raft1
```

```text
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
```

### 3C

```bash
make RUN="-run 3C" raft1
```

```text
lcz@iv-yef3xahqtc5i3z5jzmr5:~/mit6.5840/6.5840/src$ make RUN="-run 3C" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3C 
=== RUN   TestPersist13C
Test (3C): basic persistence (reliable network)...
  ... Passed --  time  3.3s #peers 3 #RPCs    98 #Ops    6
--- PASS: TestPersist13C (3.75s)
=== RUN   TestPersist23C
Test (3C): more persistence (reliable network)...
  ... Passed --  time 13.7s #peers 5 #RPCs   568 #Ops   16
--- PASS: TestPersist23C (14.36s)
=== RUN   TestPersist33C
Test (3C): partitioned leader and one follower crash, leader restarts (reliable network)...
  ... Passed --  time  1.5s #peers 3 #RPCs    48 #Ops    4
--- PASS: TestPersist33C (1.84s)
=== RUN   TestFigure83C
Test (3C): Figure 8 (reliable network)...
2026/03/18 22:15:05 6PCdkEFTs2eiMT_RlFB1: dmxsrv.reader: clnt ACu3swj2_8gbs1Wd6nbn ReadCall err read unix /tmp/6.5840-6PCdkEFTs2eiMT_RlFB1->@: read: connection reset by peer
2026/03/18 22:15:47 6PCdkEFTs2eiMT_RlFB1: dmxsrv.reader: clnt 5VPxjteaE_i4P1o9h71T ReadCall err read unix /tmp/6.5840-6PCdkEFTs2eiMT_RlFB1->@: read: connection reset by peer
  ... Passed --  time 51.8s #peers 5 #RPCs  2369 #Ops    2
--- PASS: TestFigure83C (52.30s)
=== RUN   TestUnreliableAgree3C
Test (3C): unreliable agreement (unreliable network)...
  ... Passed --  time  3.4s #peers 5 #RPCs   220 #Ops  246
--- PASS: TestUnreliableAgree3C (4.02s)
=== RUN   TestFigure8Unreliable3C
Test (3C): Figure 8 (unreliable) (unreliable network)...
  ... Passed --  time 48.0s #peers 5 #RPCs  7496 #Ops    2
2026/03/18 22:16:44 T5AgHTtizWPRzjsYwAQ6: dmxsrv.reader: clnt UkD_e2Q-b5OG3f1PBPSa ReadCall err read unix /tmp/6.5840-T5AgHTtizWPRzjsYwAQ6->@: read: connection reset by peer
--- PASS: TestFigure8Unreliable3C (48.77s)
=== RUN   TestReliableChurn3C
Test (3C): churn (reliable network)...
  ... Passed --  time 16.6s #peers 5 #RPCs  1084 #Ops    1
--- PASS: TestReliableChurn3C (17.17s)
=== RUN   TestUnreliableChurn3C
Test (3C): unreliable churn (unreliable network)...
  ... Passed --  time 16.8s #peers 5 #RPCs  1028 #Ops    1
--- PASS: TestUnreliableChurn3C (17.46s)
PASS
ok      6.5840/raft1    160.685s
```

### 3D

```bash
make RUN="-run 3D" raft1
```

```text
lcz@iv-yef3xahqtc5i3z5jzmr5:~/mit6.5840/6.5840/src$ make RUN="-run 3D" raft1
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race -run 3D 
=== RUN   TestSnapshotBasic3D
Test (3D): snapshots basic (reliable network)...
  ... Passed --  time  3.1s #peers 3 #RPCs  542 #Ops   31
--- PASS: TestSnapshotBasic3D (3.51s)
=== RUN   TestSnapshotInstall3D
Test (3D): install snapshots (disconnect) (reliable network)...
  ... Passed --  time 32.4s #peers 3 #RPCs  1794 #Ops   91
--- PASS: TestSnapshotInstall3D (32.87s)
=== RUN   TestSnapshotInstallUnreliable3D
Test (3D): install snapshots (disconnect) (unreliable network)...
  ... Passed --  time 59.8s #peers 3 #RPCs  2738 #Ops   91
--- PASS: TestSnapshotInstallUnreliable3D (60.28s)
=== RUN   TestSnapshotInstallCrash3D
Test (3D): install snapshots (crash) (reliable network)...
  ... Passed --  time 27.9s #peers 3 #RPCs  1518 #Ops  91
--- PASS: TestSnapshotInstallCrash3D (28.26s)
=== RUN   TestSnapshotInstallUnCrash3D
Test (3D): install snapshots (crash) (unreliable network)...
  ... Passed --  time 34.1s #peers 3 #RPCs  1644 #Ops  91
--- PASS: TestSnapshotInstallUnCrash3D (34.39s)
=== RUN   TestSnapshotAllCrash3D
Test (3D): crash and restart all servers (unreliable network)...
  ... Passed --  time  8.6s #peers 3 #RPCs  362 #Ops  67
--- PASS: TestSnapshotAllCrash3D (9.11s)
=== RUN   TestSnapshotInit3D
Test (3D): snapshot initialization after crash (unreliable network)...
  ... Passed --  time  2.3s #peers 3 #RPCs  80 #Ops  14
--- PASS: TestSnapshotInit3D (2.78s)
PASS
ok      6.5840/raft1    172.227s
```

<a id="cicd-sec"></a>

## 5. CI 与 Docker

- 推送/PR 到 `main` 或 `master` 时，GitHub Actions 会在 `6.5840/src` 下跑与上表同源的测试（`./mr`、`kvsrv1`、`kvsrv1/lock`、`raft1` 默认 3A–3D 全文），同样带 `-race`。
- 在 **`mit6.5840/6.5840/`** 下执行 `docker build` / `docker run`，镜像内会顺序执行同类测试。详见 `Dockerfile` 顶部注释与 `.github/workflows/ci.yml`。 

test
