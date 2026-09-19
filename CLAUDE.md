# CLAUDE.md

本文件为 Claude Code 在本仓库工作时的强制指引。

## 项目概述

DurableFlow（`com.brixdata:durableflow`）是一个持久化工作流执行引擎，基于 Java 21，使用 Maven 构建。

## 工具链（固定版本，必须严格使用）

- **OpenJDK 21.0.12.1**：Temurin `21.0.12.1+1`，安装于 `~/tools/jdk-21.0.12.1+1/Contents/Home`
- **Maven 3.9.16**：安装于 `~/tools/apache-maven-3.9.16`

在执行任何 Maven/Java 命令前，先加载本机环境脚本：

```bash
source scripts/env.sh
java -version   # 必须显示 openjdk 21.0.12.1
mvn -version    # 必须显示 Apache Maven 3.9.16，Java version: 21.0.12.1
```

不要使用系统自带的其他 JDK（如 `/Library/Java/JavaVirtualMachines/jdk-21.jdk` 的 21.0.9）运行本项目构建。

## 目录结构

```
src/main/java/        生产源码（包根：com.brixdata.durableflow）
src/test/java/        JUnit 5 测试
config/               配置目录；durableflow.example.yaml 为示例模板，
                      实际使用时复制为 durableflow.yaml（已 gitignore，不入库）
data/                 本地持久化数据目录（运行时数据已 gitignore，仅保留占位文件）
scripts/env.sh        本机 JDK/Maven 环境切换脚本
target/               Maven 构建产物（不入库）
```

## 常用命令

```bash
source scripts/env.sh
mvn test          # 运行全部测试
mvn -q test       # 静默模式，仅输出错误
mvn package       # 编译、测试并打包
mvn clean         # 清理 target/
```

## 强制工作流（每次修改必须执行，不可跳过）

**每次完成代码、配置或文档修改后，都必须依次完成以下三步，并立即生成一个独立的 Git commit：**

1. **检查当前改动**：用 `git status` 和 `git diff`（含暂存区与未暂存改动）逐项核对，确认改动范围与任务一致、没有误改或遗漏，确认没有将 `target/`、`config/durableflow.yaml`、`data/` 运行时数据等不该入库的文件纳入提交。
2. **运行与修改相关的测试**：
   - 修改了 `src/main/` 下的代码，至少运行受影响模块的测试；无法确定影响面时运行 `mvn test` 全量测试。
   - 修改了测试代码，运行对应测试类（如 `mvn test -Dtest=类名`），必要时全量运行。
   - 修改了配置/文档（`config/`、`*.md`、`pom.xml` 等），如改动涉及构建或运行行为，运行能验证该改动的命令（如 `mvn validate`、`mvn test`）；纯文档改动可说明无需测试，但前一步的改动检查不可省略。
   - 测试不通过时，先修复再提交；禁止在测试失败或未运行该运行的测试时提交。
3. **立即生成一个独立 Git commit**：
   - 一次任务的一批相关修改作为一个独立 commit，不要攒多个不相关改动合并提交，也不要在一个 commit 中混入无关文件。
   - commit message 使用简洁的祈使句，准确概括改动（如 `add workflow checkpoint storage`、`fix step timeout retry`、`update config example`）。
   - 提交后用 `git log -1 --stat` 确认提交内容正确。

未经用户明确要求，不要 push 到远程。

## 编码约定

- Java 21 语法，UTF-8 编码，缩进 4 空格。
- 新功能代码必须配套 JUnit 5 测试，测试放在与源码相同的包路径下。
- 配置项变更必须同步更新 `config/durableflow.example.yaml`。
- 新增的运行时本地数据一律写入 `data/`，不要在仓库其他位置产生本地状态文件。
