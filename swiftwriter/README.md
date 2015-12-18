# swiftWriter
## 介绍

在实时索引的场景下，必须以一种流式的方式进行索引的构建和查询，从网页被抓取到可以被查询，需要经过爬虫、mfp处理、索引build一系列的过程。swift(雨燕)系统会作为消息的中间件，将爬虫和mfp处理后的网页缓存起来，并提供给下游的引擎。它可以在提供高吞吐量和低延迟的同时，保证消息的可靠性，使得在某些组件出错的情况下，系统仍然可以正常运行。
http://baike.corp.taobao.com/index.php/Galaxy_swift#.E5.B8.AE.E5.8A.A9.E6.96.87.E6.A1.A3

## 实现原理 

基于 swift java sdk 进行开发

http://baike.corp.taobao.com/index.php/How_to_use_swift_client

```
        <dependency>
            <groupId>com.alibaba.search.swift</groupId>
            <artifactId>swift_client</artifactId>
        </dependency>

```

## 配置说明 
### 配置样例 

```
{
  "job": {
    "setting": {
      "speed": {
        "byte": 10485760
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {},
        "writer": {
          "name": "swiftwriter",
          "parameter": {
            "client_config": "zkPath=zfs://10.218.144.48:2181/swift/swift_service;",
            "writer_config": "topicName=datax_test;functionChain=HASH,hashId2partId",
            "hash_fields": [0],
            "index_names": [
              "uuid",
              "host",
              "uri",
              "src_ip",
              "src_port",
              "dst_ip",
              "method",
              "referer",
              "user_agent",
              "post_data",
              "attack_type"
            ]
          }
        }
      }
    ]
  }
}

```
### 参数说明 
* client_config
     ** 描述： 创建 swift client 参数,原封传递给 swift client 包括 zookeeper 地址等参数 
     ** 必须：是
     ** 默认值 ：无
* writer_config
    ** 描述： 创建 swift writer 参数，原封传递给 swift writer 
    ** 必须：是
    ** 默认值：无
    
* hash_fields
    ** 描述：写入时的hash字段，用于builder.setHashStr 参数，多个field 之间用空白连接
    ** 必须：否
    ** 默认值：无
* index_names
    ** 描述：索引字段集合，个数必须与 record 中的字段数个数相同，索引名与原中字段可以不同
    ** 必须：是
    ** 默认值：否
    
