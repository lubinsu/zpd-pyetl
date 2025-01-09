
```
支持数据库：MySQL、Oracle、PostgreSQL、SQLServer、iris、db2

初始化数据库脚本(MySQL版本)：scripts/repository.sql

支持的操作：异构数据库之间表对表抽取、变量传递、存储调用、SQL脚本调用、动态语句拼接、单条数据轮询同步（转换）、HTTP调用（POST、GET）、WebService调用、JavaScript代码调用
xml解析、json解析、xml、json解析后入库、调用shell脚本等，并且可在原基础上很方便的进行扩展

新功能：支持BLOB字段数据抽取、支持多线程任务
执行通过调用rest服务来发起etl任务

支持插件扩展，任务自动调度，外部传参等能力

## 系统架构

zpd-pyetl 是一个基于Python开发的ETL（Extract, Transform, Load）数据处理系统，采用模块化设计，主要包含以下核心组件：

1. **数据抽取层**：
   - 支持多种数据库（MySQL、Oracle等）的数据抽取
   - 提供HTTP、WebService等接口调用能力
   - 支持BLOB字段处理和多线程任务

2. **数据处理层**：
   - 提供XML/JSON解析功能
   - 支持动态SQL语句拼接
   - 内置JavaScript引擎支持脚本处理
   - 提供数据转换和清洗功能

3. **任务调度层**：
   - 基于cron的定时任务调度
   - 支持REST API触发任务
   - 提供任务监控和自动重启机制

4. **插件系统**：
   - 模块化插件架构，支持功能扩展
   - 提供标准插件接口
   - 内置多种数据解析插件（XML、JSON、HTTP等）

5. **日志系统**：
   - 提供统一的日志记录接口
   - 支持日志存储到MySQL数据库
   - 提供日志分级和过滤功能

## 核心功能模块

### 1. 数据库操作
- 支持主流关系型数据库
- 提供数据库连接池管理
- 支持存储过程调用
- 提供数据库表结构检查工具

### 2. 数据解析
- XML解析：支持XPath表达式
- JSON解析：支持复杂结构处理
- HTTP接口：支持RESTful API调用
- WebService：支持SOAP协议

### 3. 任务调度
- 基于cron的定时任务
- 支持任务依赖管理
- 提供任务状态监控
- 支持任务失败重试

### 4. 插件系统
- 内置常用数据解析插件
- 支持自定义插件开发

### 5. 日志系统
- 统一的日志接口
- 支持日志分级（DEBUG/INFO/WARN/ERROR）
- 支持日志存储到数据库、以及日志文件

```

# zpd-pyetl

已经可以通过一键安装脚本实现部署：zpd-pyetl.sh，详细的安装、配置文档可见：ETL工具使用文档.docx

 欢迎沟通交流：
> * 微信号：snoopy-lubin
> * 公众号：玩大数据的snoopy
> * CSDN：https://blog.csdn.net/lubinsu?type=blog
