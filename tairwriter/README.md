# DataX tairwriter 说明

-----------

## 1 快速介绍
[Tair] (http://baike.corp.taobao.com/index.php/CS_RD/tair) Tair是一个高性能，分布式，可扩展，高可靠的nosql存储系统。现支持Java，C/C++版本和restful的客户端。

Tair后台有mdb/ldb两种存储引擎，mdb是类似memcache的内存kv，ldb是基于leveldb的持久化存储引擎，请根据你们的业务场景选择（可以咨询@Tair答疑）

TairWriter的定位在使用通用的接口写入mdb/ldb的方式。如果有从odps->tair的大数据量导入需求，请使用fastdump（后续也将接入datax, 联系@丰茂）

## 2 实现原理
TairWriter实现的比较简单，主要就是使用put/prefixputs/setCount/prefixSetCounts接口，将数据写入tair.

## 3 功能说明

### 3.1 配置样例
* 从ODPS到Tair导入的数据(使用put)。

```json
```

* 从ODPS到Tair导入的数据（使用prefixput）。
```json
```

### 3.2 参数说明

* **configId**
  * 描述：Tair集群ID <br />
  * 必选：是 <br />
  * 默认值：无 <br />

* **namespace**
  * 描述：Tair集群Namespace <br />
  * 必选：是 <br />
  * 默认值：无 <br />


### 3.3 类型转换
tair不是强类型的NoSQL数据库，但是可以支持java的数据类型，如果使用tair的java客户端，tair中的key value都可以是java中的任何Serilizable数据类型 <br/>
但是如果使用c++客户端（或者python lua等基于c++的客户端），没有数据类型的概念，读出来全部都是字符串 <br/>
配置文件中的LANGUAGE参数决定了导入的数据格式(java or c++)<br/>

如果language为java 那么数据类型的对应关系如下：
| DataX 内部类型| Tair(java) 数据类型    |
| -------- | ----- |
| Long     |long |
| Double   |double |
| String   |string |
| Date     |datetime |
| Boolean  |bool |


## 4 插件特点

### 4.1 写入幂等性

### 4.2 任务重跑和failover


## 5 性能报告 (TODO)


## 6 FAQ
