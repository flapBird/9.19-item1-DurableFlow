# 本地数据目录

DurableFlow 运行时产生的持久化数据（工作流状态、检查点、分段数据文件等）存放在本目录。

- 目录内容已被 `.gitignore` 忽略，仅保留本占位文件；
- 默认路径由 `config/durableflow.yaml` 中的 `durableflow.storage.data-dir` 指定，默认值为 `./data`；
- 如需重置本地状态，停止引擎后删除本目录下除 `README.md`、`.gitkeep` 之外的所有内容。
