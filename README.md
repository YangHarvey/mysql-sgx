# Run MySQL on Occlum

[`MySQL`](https://www.mysql.com/) is a widely used open-source relational database management system (RDBMS).

### Preinstall dependencies
Related dependencies: libnuma-dev libboost-all-dev boost-1.77.0
```
./preinstall_deps.sh
```

### Download and build MySQL
```
./dl_and_build_mysql.sh
```
This command downloads MySQL-8.0.31 source code and builds from it.
When completed, all MySQL related binaries and tools are installed.

### Run server and client

#### Initialize and start the MySQL server
```
./run_mysql_server.sh
```
This command initializes and runs the server (using `mysqld`) in Occlum.
When completed, the server starts to wait connections.

#### Start the MySQL client and send simple queries
```
./run_mysql_client.sh
```
This command starts the client (using `mysql`) in Occlum and test basic query SQLs.

The network protocol between client and server uses uds(unix domain socket) by default.
More configuration can be tuned and applied in `my.cnf`.

/projects/linux-sgx/linux/installer/deb/local_repo_tool/../sgx_debian_local_repo


## 检查配置

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'server_id';
-- SHOW VARIABLES LIKE 'gtid_mode';
```

## 创建replicate用户
```bash
/usr/local/mysql/bin/mysql -h 127.0.0.1 -u root 
create database benchbase;
```

```sql
CREATE USER 'replicator'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
FLUSH PRIVILEGES;
exit
```

