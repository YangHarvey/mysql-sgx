#!/bin/bash

# --- 配置 ---
MYSQL_EXECUTABLE="/usr/local/mysql/bin/mysql"
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="3306"
MYSQL_ROOT_USER="root"
DB_NAME="benchbase"
REPL_USER="replicator"
REPL_PASSWORD="password"

echo "尝试以 root 用户连接 MySQL..."

# 执行 SQL 命令
$MYSQL_EXECUTABLE -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_ROOT_USER" <<EOF
-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS \`$DB_NAME\`;

-- 创建用户（如果不存在）
CREATE USER IF NOT EXISTS '$REPL_USER'@'%' IDENTIFIED BY '$REPL_PASSWORD';

-- 授权复制权限
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '$REPL_USER'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 显示用户信息（用于确认）
SELECT USER, HOST FROM mysql.user WHERE USER = '$REPL_USER';
SHOW GRANTS FOR '$REPL_USER'@'%';
EOF

# 检查状态
STATUS=$?
if [ $STATUS -eq 0 ]; then
    echo "-------------------------------------"
    echo "✅ MySQL 命令成功执行。"
    echo "📦 数据库 '$DB_NAME' 已存在或已创建。"
    echo "👤 用户 '$REPL_USER'@'%' 已创建或已存在。"
    echo "🔐 权限已授予：REPLICATION SLAVE, REPLICATION CLIENT。"
    echo "-------------------------------------"
else
    echo "-------------------------------------"
    echo "❌ 错误：MySQL 命令执行失败，退出状态码: $STATUS"
    echo "🛠 请检查 MySQL 服务、密码设置或 SQL 语法。"
    echo "-------------------------------------"
    exit $STATUS
fi

# 正常退出
exit 0