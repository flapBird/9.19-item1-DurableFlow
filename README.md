# 9.19-item1-DurableFlow

持久化工作流执行引擎（Java 21 + Maven）。工作流定义、执行状态与定时信息全部持久化在本地文件系统，不依赖数据库、消息队列或其他外部服务。

## 环境要求

- OpenJDK 21.0.12.1（Temurin）
- Maven 3.9.16

本机已安装在 `~/tools/` 下，构建前加载环境：

```bash
source scripts/env.sh
```

## 构建与测试

```bash
mvn test      # 运行全部测试
mvn package   # 编译、测试并打包（shade 生成可执行 jar）
```

## 命令行使用

```bash
java -jar target/durableflow-0.1.0-SNAPSHOT.jar [--config config/durableflow.yaml] [--data-dir ./data] <command>

# 提交工作流并运行至完成（key=value 作为工作流输入）
java -jar target/durableflow-0.1.0-SNAPSHOT.jar submit examples/order-flow.json express=true

# 恢复所有未完成工作流并运行至完成（重启后继续，不重新计时）
java -jar target/durableflow-0.1.0-SNAPSHOT.jar recover

# 查看执行状态（只读，不触发执行）
java -jar target/durableflow-0.1.0-SNAPSHOT.jar status [instanceId]
java -jar target/durableflow-0.1.0-SNAPSHOT.jar list
```

`submit` / `recover` 退出码：全部实例成功为 0，否则为 1。

## 工作流定义

工作流定义为一个 JSON 文件，步骤构成 DAG：

```json
{
  "id": "order-flow",
  "steps": [
    {"id": "reserve-stock", "type": "TASK", "handler": "log",
     "args": {"message": "reserve-stock"},
     "retry": {"maxAttempts": 3, "backoffMillis": 500, "multiplier": 2.0},
     "compensationHandler": "log", "compensationArgs": {"message": "cancel-reserve-stock"}},
    {"id": "wait-warehouse", "type": "WAIT", "waitMillis": 1000, "dependsOn": ["reserve-stock"]},
    {"id": "express-ship", "type": "TASK", "handler": "log",
     "dependsOn": ["wait-warehouse"], "condition": "input.express == true"}
  ]
}
```

| 字段 | 说明 |
| --- | --- |
| `type` | `TASK`（执行处理器）或 `WAIT`（定时等待 `waitMillis` 毫秒） |
| `handler` / `args` | 处理器名称与参数；内置处理器：`echo`、`sleep`、`fail`、`log` |
| `dependsOn` | 依赖步骤列表。串行 = 依赖链；并行 = 多步骤共享前置；全部依赖成功（或跳过）后才可执行 |
| `condition` | 条件表达式，为假时步骤被跳过（条件分支）。支持 `input.<key>`、`input.<key> == / != <值>`、`steps.<id>.result == <值>`、`!` 前缀 |
| `retry` | 重试策略：`maxAttempts`（含首次）、`backoffMillis`（退避基数）、`multiplier`（倍增因子），未配置时用引擎默认值 |
| `compensationHandler` / `compensationArgs` | 补偿操作，后续步骤最终失败时按完成逆序执行 |

同时运行的任务数量由配置 `execution.max-concurrency` 限制。

## 持久化与崩溃恢复

- **事件日志**：每个实例的所有状态迁移（提交、就绪、开始、成功、失败、跳过、补偿等）以 JSON 行追加写入 `data/workflows/<instanceId>.journal`，每次写入后 `fsync` 落盘。重启后通过顺序回放事件重建内存状态（事件溯源）。
- **崩溃容忍**：进程可能在任务开始前、执行中、执行后或状态写入过程中被终止。日志按行校验，崩溃截断的最后一行被丢弃，之前的完整事件仍然有效；崩溃时处于 `RUNNING` 的步骤恢复后重新派发。
- **定时信息**：定时等待与重试退避持久化的是**绝对唤醒时间**（`resumeAt`）。进程在等待期间被关闭后，重启按原计划时间继续：已过期立即触发，未过期只等待剩余时间，不会重新计时。
- **恢复入口**：`recover` 命令（或 `WorkflowEngine.recover()`）扫描数据目录，恢复所有未完成实例：就绪/执行中的步骤重新派发，等待中的步骤按原时间重新挂定时器，补偿中的工作流继续执行剩余补偿。

## 重复执行处理（幂等）

- **工作流级**：实例 id 由 `SHA-256(定义 id + 规范化输入)` 派生。同一工作流在相同输入下重复提交返回已有实例，不会重复执行，最终结果保持稳定。
- **步骤级**：处理器通过 `StepContext.executeIdempotent(suffix, action)` 包裹副作用操作。幂等键为 `实例id:步骤id[:suffix]`（不含尝试次数），执行结果先落盘到 `data/idempotency.journal` 再返回。任务“实际执行成功但完成状态尚未来得及记录”时，恢复后重新派发会命中幂等缓存，直接返回原结果而不重复副作用。
- **注意**：副作用发生与结果落盘之间仍存在极小的崩溃窗口，因此写外部系统的处理器应以幂等键作为外部请求的去重键，实现端到端幂等。

## 失败重试与补偿

- 步骤失败后按指数退避自动重试（`backoffMillis * multiplier^(n-1)`，上限 5 分钟），重试次数耗尽后步骤进入 `DEAD`，工作流失败。
- 工作流失败时，已成功且配置了 `compensationHandler` 的步骤按**完成时间逆序**依次补偿；补偿本身按同一重试策略重试，补偿期间崩溃可在重启后继续；补偿重试耗尽则工作流进入 `COMPENSATION_FAILED`。

## 配置

复制 `config/durableflow.example.yaml` 为 `config/durableflow.yaml`（不入库）后修改；文件不存在时使用默认值。支持项：节点标识、数据目录、最大并发、默认重试策略，详见示例文件注释。

## 目录

- `src/main/java/` 源码（`model` 定义与状态、`persistence` 事件日志与幂等存储、`engine` 执行引擎、`config` 配置）
- `src/test/java/` 测试（正常执行、并行分支、任务重试、定时恢复、执行中崩溃、补偿失败等场景）
- `examples/order-flow.json` 示例工作流（串行 + 并行 + 条件分支 + 定时等待 + 补偿）
- `config/durableflow.example.yaml` 示例配置（复制为 `durableflow.yaml` 使用）
- `data/` 本地持久化数据目录
