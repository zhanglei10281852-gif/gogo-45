# Bug Reproduction

## 包的性质

当前 test_model_fix 保存的是被测模型修复后的结果源码，不是初始含 Bug 源码。要复现原始缺陷，必须检出下面固定的 parent SHA；不要在当前修复结果源码上期待重新出现修复前失败。生成系统使用的可信验证补丁和完整验证日志仅在本地留存，不提交到结果分支。

## 问题现象

同一工作者有多个 slot 时，若多个任务被安排到不同 slot 并在时间上重叠，每个任务单独看都满足资源限制，但计划中的并发 CPU 和内存总量会超过该工作者真实容量。先不要修改代码。请调查资源为何没有跨 slot 聚合，给出可核验证据、完整因果链，并定位具体 Go 文件和符号。

## 含 Bug 版本

- 仓库：zhanglei10281852-gif/gogo-45
- 仓库地址：https://github.com/zhanglei10281852-gif/gogo-45.git
- parent SHA：3f43a30a9a9698b4ee0298d098d0a989e2154a3b

## 复现步骤

```bash
git clone -- https://github.com/zhanglei10281852-gif/gogo-45.git bug-repro
cd bug-repro
git checkout --detach 3f43a30a9a9698b4ee0298d098d0a989e2154a3b
go test ./internal/planner -run "^TestBuildReservesAggregateWorkerResourcesAcrossSlots$" -count=1 -v
```

## 双架构完整错误信息

### linux/amd64

- 容器内复现预期退出码：1
- 容器内复现实际退出码：1

stdout：

```text
$ go test ./internal/planner -run "^TestBuildReservesAggregateWorkerResourcesAcrossSlots$" -count=1 -v
=== RUN   TestBuildReservesAggregateWorkerResourcesAcrossSlots
    planner_test.go:49: b starts at 2023-11-14 22:13:20 +0000 UTC, want 2023-11-14 22:13:30 +0000 UTC
--- FAIL: TestBuildReservesAggregateWorkerResourcesAcrossSlots (0.00s)
FAIL
FAIL	QueueForge/internal/planner	0.002s
FAIL

```

stderr：

```text
(empty)
```

### linux/arm64

- 容器内复现预期退出码：1
- 容器内复现实际退出码：1

stdout：

```text
$ go test ./internal/planner -run "^TestBuildReservesAggregateWorkerResourcesAcrossSlots$" -count=1 -v
=== RUN   TestBuildReservesAggregateWorkerResourcesAcrossSlots
    planner_test.go:49: b starts at 2023-11-14 22:13:20 +0000 UTC, want 2023-11-14 22:13:30 +0000 UTC
--- FAIL: TestBuildReservesAggregateWorkerResourcesAcrossSlots (0.01s)
FAIL
FAIL	QueueForge/internal/planner	0.124s
FAIL

```

stderr：

```text
(empty)
```

## 通过条件

定位 internal/planner/planner.go 的 Build、workerSlot、chooseSlot；解释每个逻辑 slot 复制完整工作者容量且只推进被选 slot 的 freeAt，导致重叠任务资源从未汇总的完整机制；有证据且目标仓库零改动。
