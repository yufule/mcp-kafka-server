# MCP Kafka工具

这是一个基于MCP（Model Context Protocol）协议的Kafka工具，可以通过大模型连接Kafka，提供以下功能：

1. 查询指定topic的信息
2. 往指定topic里面读写数据
3. 回放指定时间，指定topic的数据

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方式

### 本地模式（STDIO）

直接运行`mcp_kafka_server.py`文件：

```bash
python mcp_kafka_server.py
```

然后在MCP客户端（如Claude、Cherry Studio等）中连接本地MCP服务器。

### Web服务模式（SSE）

运行`mcp_kafka_sse_server.py`文件启动Web服务：

```bash
python mcp_kafka_sse_server.py
```

服务将启动在 http://localhost:8000，MCP端点为 http://localhost:8000/mcp

然后在MCP客户端中连接这个远程MCP服务器。

## 主要功能

### 1. 列出所有Topic

```python
list_topics()
```

返回Kafka中所有可用的Topic列表。

### 2. 获取Topic信息

```python
get_topic_info(topic_name)
```

获取指定Topic的详细信息，包括分区数量、每个分区的起始和结束偏移量以及消息数量。

### 3. 发送消息

```python
send_message(topic_name, message, key)
```

向指定Topic发送消息，message可以是JSON字符串或普通字符串，key为可选参数。

### 4. 读取消息

```python
read_messages(topic_name, max_messages, timeout_ms)
```

从指定Topic读取最新的消息，max_messages指定最大读取消息数（默认10条），timeout_ms指定超时时间（默认1000毫秒）。

### 5. 回放历史消息

```python
replay_messages(topic_name, start_time, end_time, max_messages)
```

回放指定时间段内的消息，start_time和end_time为ISO格式的时间字符串（如'2023-01-01T00:00:00'），max_messages指定最大回放消息数（默认100条）。

## 配置Kafka连接

默认连接到`localhost:9092`，如需修改连接地址，请在`KafkaManager`类的初始化参数中修改`bootstrap_servers`。