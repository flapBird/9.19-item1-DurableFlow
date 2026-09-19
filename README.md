# 9.19-item1-DurableFlow

持久化工作流执行引擎（Java 21 + Maven）。

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

## 目录

- `src/main/java/` 源码
- `src/test/java/` 测试
- `config/durableflow.example.yaml` 示例配置（复制为 `durableflow.yaml` 使用）
- `data/` 本地持久化数据目录
