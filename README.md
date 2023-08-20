# Npool go service app template

[![Test](https://github.com/NpoolPlatform/npool-scheduler/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/NpoolPlatform/npool-scheduler/actions/workflows/main.yml)

[目录](#目录)
- [功能](#功能)
- [命令](#命令)
- [步骤](#步骤)
- [最佳实践](#最佳实践)
- [关于mysql](#关于mysql)
- [GRPC](#grpc)

-----------
### 功能
#### 用户充值入账
- staker会每隔1分钟检测一下用户充值地址的余额变化，当检测到变化后会更新accounts数据库中该地址的充值记录，同时在ledger_manager数据库中为用户创建一条deposit的detail，更新用户的general总账
#### 通知/公告发送
#### 手续费充值
- staker会按照顺序遍历不同类型的地址余额并根据余额值判断是否为其充值手续费
#### limitation转账
- staker会每隔4小时查询热钱包的余额，当余额超出热钱包的限额的两倍，staker会将超出限额的部分转入用户冷钱包
#### 资金归集
- staker会每隔4小时查询一下支付地址、充值地址的余额，当检测到的余额大于平台设置的collect amount, staker会触发一次归集，将该地址的金额转入平台冷钱包，归集完成后，该地址会冷却一小时
#### 用户提现交易处理
#### 订单处理流程
#### 商品收益分账
- 商品收益地址：每个商品都有一个收益地址，该地址与商品对应的算力机绑定，算力机产生的收益会直接进入该收益地址
- 分账流程：staker在每天UTC 0点检查该地址余额，大于商品对应币种reserved amount，就会触发分账
  - 计算用户部分收益，计算平台部分收益
  - 核验InService的订单与库存表InService数量一致后，会将商品收益转到用户收益热钱包
  - 以上交易成功后，staker会将属于平台部分的收益从用户收益热钱包转到平台冷钱包，同时触发bookkeeping
- 用户部分收益：商品总收益 * (该商品InService的订单份 / 该商品库存total)
- 平台部分收益：商品总收益 * (未售出份数 / 该商品库存total) + 用户部分收益 * 技术付服务费
- bookkeeping：商品收益detail/general记账，用户收益detail/general记账，商品profits记账


### 命令
* make init ```初始化仓库，创建go.mod```
* make verify ```验证开发环境与构建环境，检查code conduct```
* make verify-build ```编译目标```
* make test ```单元测试```
* make generate-docker-images ```生成docker镜像```
* make npool-scheduler ```单独编译服务```
* make npool-scheduler-image ```单独生成服务镜像```
* make deploy-to-k8s-cluster ```部署到k8s集群```

### 最佳实践
* 每个服务只提供单一可执行文件，有利于docker镜像打包与k8s部署管理
* 每个服务提供http调试接口，通过curl获取调试信息
* 集群内服务间direct call调用通过服务发现获取目标地址进行调用
* 集群内服务间event call调用通过rabbitmq解耦

### 关于mysql
* 创建app后，从app.Mysql()获取本地mysql client
* [文档参考](https://entgo.io/docs/sql-integration)

### GRPC
* [GRPC 环境搭建和简单学习](./grpc.md)
