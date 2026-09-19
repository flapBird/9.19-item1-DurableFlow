# 本机工具链环境脚本
# 用法（在项目根目录执行）：source scripts/env.sh
#
# 固定使用 OpenJDK 21.0.12.1 (Temurin) 与 Maven 3.9.16。

export JAVA_HOME="$HOME/tools/jdk-21.0.12.1+1/Contents/Home"
export M2_HOME="$HOME/tools/apache-maven-3.9.16"
export PATH="$M2_HOME/bin:$JAVA_HOME/bin:$PATH"
