# yugong

[TOC]


## 目的

*   SQL Server -> MySQL 的一致性检查（CHECK）
*   MySQL -> SQL Server 的回滚（SYNC）

## 获得 yugong jar 包

方法一：编译 yugong jar 包：

```
git git@gitlab.yeshj.com:hjarch-practice/yugong.git
cd yugong
mvn clean package
cp target/yugong-shaded.jar .
```

方法二：
不想编译的同学，
直接点击 [Tags · HJArch-Internal / yugong · GitLab](https://gitlab.yeshj.com/hjarch-practice/yugong/tags?search=release-binary&sort=updated_desc) ,
找到里面最新的版本，里面有 jar 包下载。


## 配置文件

有两个配置文件：

*   properties，配置数据库信息和作业信息
*   YAML 配置文件，做 Translator 定制化


修改配置文件，可以参考其他产线已经在用的配置：
[Files · master · HJArch-Internal / yugong-conf · GitLab](https://gitlab.yeshj.com/hjarch-practice/yugong-conf/tree/master)


## 运行

HJ 使用的 yugong 已经改为 fat jar 模式运行，摒弃了官方的打包流程。
将生成的 fat jar `yugong-shaded.jar` 拷贝到服务器，即可运行。


运行参数：

*   -c：使用的 yugong properties，配置数据库信息和作业信息
*   -y：使用的 YAML 配置文件，做 Translator 定制化


运行命令：

```
java -jar yugong-shaded.jar -c sync-mssql-mysql.properties -y mssql-mysql.yaml
```

PS：如果想优化运行速度，可以加入 `JAVA_OPTIONS`：

```
JAVA_OPTIONS=("-Xms2048m" "-Xmx3072m" "-Xmn1024m" "-XX:SurvivorRatio=2" "-XX:PermSize=96m" "-XX:MaxPermSize=256m" "-Xss256k" "-XX:-UseAdaptiveSizePolicy" "-XX:MaxTenuringThreshold=15" "-XX:+DisableExplicitGC" "-XX:+UseConcMarkSweepGC" "-XX:+CMSParallelRemarkEnabled" "-XX:+UseCMSCompactAtFullCollection" "-XX:+UseFastAccessorMethods" "-XX:+UseCMSInitiatingOccupancyOnly" "-XX:+HeapDumpOnOutOfMemoryError")
java -server $JAVA_OPTIONS -jar yugong-shaded.jar -c sync-mssql-mysql.properties -y mssql-mysql.yaml
```


## 运行之后的错误检查

所有错误日志在 `logs` 目录下面，范例如下：

```
logs
├── HJ_OpenPlatform.UserGroups  # 每张表的日志
│   ├── applier.log  # Applier 日志
│   ├── check.log  # 一致性检查日志
│   ├── extractor.log  # Extractor 日志
│   └── table.log  # 表操作日志
└── yugong
    └── table.log  # yugong 的系统日志
```

运行之后，需要重点观察 `check.log` 和 `table.log` 确保里面没有 `ERROR` 信息。

可以通过 `grep -r ERROR logs` 检查错误。

每次操作会记录进度，用来断点续接。想重新开始跑应用，
需要删除当前目录下的 `logs` / `positioner_data`。


## Quick Start

获取 yugong-shaded.jar 之后，做一下操作可以快速使用 yugong：


*   配置自己的配置文件
    *   从 `yugong-conf` 里面拷贝 [dict-dev/check-mssql-mysql.properties · master · HJArch-Internal / yugong-conf · GitLab](https://gitlab.yeshj.com/hjarch-practice/yugong-conf/blob/master/dict-dev/check-mssql-mysql.properties)
    *   从 `yugong-conf` 里面拷贝 [dict-dev/mssql-mysql.yaml · master · HJArch-Internal / yugong-conf · GitLab](https://gitlab.yeshj.com/hjarch-practice/yugong-conf/blob/master/dict-dev/mssql-mysql.yaml)
*   修改 properties 里面的数据库配置：

    ```
    yugong.database.source.url=
    yugong.database.source.username=
    yugong.database.source.password=
    yugong.database.target.url=
    yugong.database.target.username=
    yugong.database.target.password=
    ```
*   修改 properties 里面的表配置，添加需要迁移的表（yugong 是白名单模式）：

    ```
    yugong.table.white=
    ```
*   修改 properties 里面的运行模式，CHECK（检查模式），FULL（写入模式）：

    ```
    yugong.table.mode=
    ```
*   运行

## Tips

odjbc missing:

>   ould not resolve dependencies for project com.taobao.yugong:yugong:jar:1.2.0:
>   The following artifacts could not be resolved: com.oracle:ojdbc14:jar:10.2.0.4.0,
>   com.alibaba.otter:canal.client:jar:1.0.25-SNAPSHOT:
>   Failure to find com.oracle:ojdbc14:jar:10.2.0.4.0 in
>   http://maven.aliyun.com/nexus/content/groups/public/ was cached in the local repository,
>   resolution will not be reattempted until the update interval of maven.aliyun.com
>   has elapsed or updates are forced

```
open http://www.oracle.com/technetwork/apps-tech/jdbc-10201-088211.html
# download ojbc14_g.jar
mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc14 -Dversion=10.2.0.4.0 -Dpackaging=jar -Dfile=ojdbc14_g.jar -DgeneratePom=true
```

canal-client missing:

>   Could not resolve dependencies for project com.taobao.yugong:yugong:jar:1.2.0: Failure to find
>   com.alibaba.otter:canal.client:jar:1.0.25-SNAPSHOT in
>   https://oss.sonatype.org/content/repositories/snapshots was cached in the local repository,
>   resolution will not be reattempted until the update interval of sonatype has elapsed or updates are
>   forced

```
git clone https://github.com/alibaba/canal
cd canal
git checkout 2cebfa0
mvn clean install
```

## 需要帮助怎么办？


请联系作者 **3D**，CCTalk 找我「狄敬超」。


相关文章：

*   [从 SQL Server 到 MySQL （一）：异构数据库迁移 - Log4D](https://blog.alswl.com/2018/03/sql-server-migration-1/) 
*   [从 SQL Server 到 MySQL（二）：在线迁移，空中换发动机 - Log4D](https://blog.alswl.com/2018/05/sql-server-migration-2/)
*   [从 SQL Server 到 MySQL（三）：愚公移山 - 开源力量 - Log4D](https://blog.alswl.com/2018/06/sql-server-migration-3/)
