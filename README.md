# 9.19-item1-DurableFlow

DurableFlow（`com.brixdata:durableflow`）是一个持久化工作流执行引擎，基于 Java 21 + Maven。
工作流定义、执行状态与定时信息全部持久化在本地文件系统，不依赖数据库、消息队列或其他外部服务。

## 环境要求

- OpenJDK 21.0.12.1（Temurin）
- Maven 3.9.16

本机已安装在 `~/tools/` 下，构建前加载环境：

```bash
source scripts/env.sh
```

## 构建与测试

```bash
mvn test      # 运行测试
mvn package   # 打包
```

## 命令行使用

```bash
# 构建可执行 jar（target/durableflow.jar，含全部依赖）
mvn -q -DskipTests package

# 提交工作流并等待执行完成
java -jar target/durableflow.jar submit examples/order-flow.json --input '{"vip": true}' --wait

# 提交后不等待（进程退出后工作流状态已持久化）
java -jar target/durableflow.jar submit examples/order-flow.json

# 重新启动：恢复所有未完成的工作流并执行到终态
java -jar target/durableflow.jar recover --wait

# 查看状态
java -jar target/durableflow.jar status <runId>
java -jar target/durableflow.jar list
```

全局选项：`--data-dir <路径>`、`--max-concurrency <n>`、`--config <路径>`。
默认读取 `config/durableflow.yaml`（由 `config/durableflow.example.yaml` 复制而来，若存在），命令行选项优先。

## 工作流定义

工作流是一个 JSON 文件：一组带依赖关系（DAG）的步骤。

```json
{
  "name": "order-flow",
  "steps": [
    {"id": "check-stock", "type": "TASK", "handler": "echo",
     "retry": {"maxAttempts": 3, "initialBackoffMs": 500, "multiplier": 2.0, "maxBackoffMs": 5000},
     "compensation": {"handler": "echo", "config": {"message": "释放库存"}}},
    {"id": "wait-payment", "type": "WAIT", "waitMs": 2000, "dependsOn": ["check-stock"]},
    {"id": "charge", "type": "TASK", "handler": "counter", "dependsOn": ["wait-payment"]},
    {"id": "gift", "type": "TASK", "handler": "echo", "dependsOn": ["charge"],
     "condition": "input.vip == true"}
  ]
}
```

- **串行**：通过 `dependsOn` 依赖链表达；只有全部依赖完成后步骤才进入执行状态。
- **并行**：多个步骤依赖同一前置步骤（或无依赖）即并行执行；同时运行的步骤数受
  `max-concurrency` 限制。
- **条件分支**：`condition` 表达式为假时步骤被跳过；依赖全部被跳过的步骤会级联跳过。
  语法：`input.<path> <op> <literal>` 或 `steps.<stepId>.<path> <op> <literal>`，
  运算符 `== != > >= < <=`，字面量支持数字、布尔、null、字符串。
- **定时等待**：`"type": "WAIT"` + `waitMs`。唤醒时间（绝对时间戳）在首次调度时持久化，
  进程重启后按原计划唤醒，不会重新计时。
- **失败重试**：`retry` 配置最大尝试次数（含首次）与指数退避
  （`initialBackoffMs * multiplier^(n-1)`，上限 `maxBackoffMs`）。
  重试的唤醒时间同样持久化，重启后按原计划继续。

### 内置 handler

| handler | 说明 |
| --- | --- |
| `echo` | 返回 `config.message`（缺省 `"echo"`） |
| `fail` | 总是失败，用于演示重试与补偿 |
| `flaky` | 前 `config.failTimes`（默认 2）次尝试失败，之后成功 |
| `sleep` | 休眠 `config.ms` 毫秒 |
| `counter` | 将 `data-dir/counters/<name>.txt` 计数器加一并返回新值（幂等） |

自定义 handler 通过 Java API 注册：`new WorkflowEngine(config, Map<String, StepHandler>)`，
handler 实现 `Object execute(StepContext ctx)`，可通过 `ctx.config()`、`ctx.input()`、
`ctx.stepResults()` 获取上下文。

## 持久化与崩溃恢复

每个工作流运行对应数据目录下的两个文件：

- `data/workflows/<runId>.journal`：**追加式事件日志**（每行一个 JSON 事件，写入即 fsync）。
  所有状态变迁（提交、步骤开始/完成/失败、重试排期、定时唤醒、补偿、终态）先写日志再生效。
- `data/workflows/<runId>.effects`：**幂等效果存储**，记录已发生的副作用及其结果。

重启时引擎扫描日志文件、重放事件重建状态，未完成的工作流自动继续：

| 崩溃时机 | 恢复行为 |
| --- | --- |
| 步骤开始前 | `STEP_STARTED` 已落盘，恢复后按同一尝试号与幂等键重新执行 |
| 步骤执行中 | 恢复后重新执行该尝试；副作用经幂等效果存储去重（见下） |
| 步骤完成后 | `STEP_COMPLETED` 已落盘，恢复后直接推进后续步骤 |
| 等待重试/定时唤醒期间 | 唤醒的绝对时间已落盘，恢复后按原计划触发，不重新计时 |
| 状态写入过程中 | 日志尾部可能残留半行记录，重放时自动忽略 |

## 重复执行防护（幂等）

步骤"已实际执行成功但完成状态尚未落盘"时进程崩溃，恢复后该步骤会以**相同的幂等键**
（`runId:stepId:attempt`）重新执行。handler 通过 `StepContext.executeIdempotent(...)`
执行副作用：副作用结果先写入效果存储并 fsync，之后才记录步骤完成事件；恢复重试时
直接返回已记录的结果，副作用不会重复发生。因此同一工作流在相同输入下，无论是否
经历崩溃恢复，最终结果保持一致（要求 handler 逻辑本身是确定性的）。

## 补偿（saga 回滚）

步骤可通过 `compensation` 配置补偿操作。当某步骤重试耗尽最终失败时，引擎等待其余
在执行/等待中的步骤收尾，然后对**已成功完成且定义了补偿**的步骤按**完成顺序的逆序**
逐个执行补偿；补偿失败按其自身 `retry` 策略重试，重试耗尽的补偿会被记录
（`failedCompensations`）并继续执行剩余补偿。补偿进度同样持久化，崩溃恢复后从断点继续。
全部补偿结束后工作流进入 `FAILED` 终态。

## 目录

- `src/main/java/` 源码（`model` 定义模型 / `persistence` 日志与效果存储 / `engine` 引擎 / `builtin` 内置 handler / `config` 配置）
- `src/test/java/` 测试（正常执行、并行与并发限制、条件分支、重试退避、定时恢复、
  执行中崩溃、幂等防重、结果稳定性、补偿与补偿失败、CLI）
- `config/durableflow.example.yaml` 示例配置（复制为 `durableflow.yaml` 使用）
- `examples/order-flow.json` 示例工作流定义
- `data/` 本地持久化数据目录
