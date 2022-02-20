# 安装 tair-cli

### 运行的必要依赖
* java 1.8+
* docker
* docker-compose

### 安装
```shell
# macos 安装依赖
$ brew install openjdk@11
$ brew install --cask docker
$ brew install docker-compose

# 检查java版本
$ java -version
openjdk version "11.0.11" 2021-04-20
OpenJDK Runtime Environment GraalVM CE 21.1.0 (build 11.0.11+8-jvmci-21.1-b05)
OpenJDK 64-Bit Server VM GraalVM CE 21.1.0 (build 11.0.11+8-jvmci-21.1-b05, mixed mode, sharing)

# 安装tair-cli 并启动grafana dashboard
$ cd /path/to
$ unzip tair-cli-release.zip
$ cd tair-cli/dashboard
$ docker-compose up -d
```

### 运行示例
```shell
$ cd /path/to/tair-cli/bin
# 生成rdb
$ ./tair-cli --source redis://host:port?authPassword=pass --convert > dump.rdb

# 打开浏览器 http://localhost:3000/d/monitor/monitor. 用户名tair-cli， 密码tair-cli登录grafana
# 监控tair服务器, 执行完下述命令后，刷新http://localhost:3000/d/monitor/monitor 看各种监控信息
$ ./tair-monitor --source redis://host:port?authPassword=pass
```

### 从源码安装

#### 源码安装的必要依赖
* java 1.8+
* maven 3.3+

```shell
$ cd /path/to/source
$ git clone https://github.com/leonchen83/tair-cli.git
$ cd tair-cli
$ mvn clean install
$ cd target/tair-cli-release/tair-cli/bin
$ ./tair-cli --version
```