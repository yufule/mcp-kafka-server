#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mcp.server.fastmcp import FastMCP
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import json
import datetime
from dateutil import parser
from typing import List, Dict, Any, Optional, Union
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 初始化 FastMCP 服务器
mcp = FastMCP("MCP_Kafka_Tools")

class KafkaManager:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.admin_client = None
        
        try:
            # 初始化生产者
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # 初始化管理客户端
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers
            )
            
            logger.info(f"成功连接到Kafka服务器: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"连接Kafka服务器失败: {str(e)}")
            raise
    
    def get_topic_list(self):
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
        topics = list(consumer.topics())
        consumer.close()
        return topics
    
    def get_topic_info(self, topic_name: str):
        try:
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
            
            # 检查Topic是否存在
            topics = consumer.topics()
            if topic_name not in topics:
                logger.warning(f"Topic {topic_name} 不存在")
                return {"error": f"Topic {topic_name} 不存在"}
            
            # 获取分区信息
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                return {"error": f"无法获取Topic {topic_name} 的分区信息"}
            
            # 获取每个分区的偏移量信息
            partition_info = []
            for partition in partitions:
                tp = TopicPartition(topic_name, partition)
                # 获取分区的起始偏移量
                consumer.assign([tp])
                consumer.seek_to_beginning(tp)
                beginning_offset = consumer.position(tp)
                
                # 获取分区的结束偏移量
                consumer.seek_to_end(tp)
                end_offset = consumer.position(tp)
                
                partition_info.append({
                    "partition": partition,
                    "beginning_offset": beginning_offset,
                    "end_offset": end_offset,
                    "message_count": end_offset - beginning_offset
                })
            
            consumer.close()
            
            return {
                "topic_name": topic_name,
                "partition_count": len(partitions),
                "partitions": partition_info,
                "total_messages": sum(p["message_count"] for p in partition_info)
            }
        except Exception as e:
            logger.error(f"获取Topic信息失败: {str(e)}")
            return {"error": f"获取Topic信息失败: {str(e)}"}
    
    def send_message(self, topic_name: str, message: Union[Dict, str], key: str = None):
        try:
            # 检查Topic是否存在，如果不存在则创建
            topics = self.get_topic_list()
            if topic_name not in topics:
                logger.info(f"Topic {topic_name} 不存在，尝试创建")
                try:
                    new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                    self.admin_client.create_topics([new_topic])
                    logger.info(f"成功创建Topic: {topic_name}")
                except Exception as e:
                    logger.error(f"创建Topic失败: {str(e)}")
                    return {"error": f"创建Topic失败: {str(e)}"}
            
            # 发送消息
            key_bytes = key.encode('utf-8') if key else None
            future = self.producer.send(topic_name, message, key=key_bytes)
            record_metadata = future.get(timeout=10)
            
            return {
                "status": "success",
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }
        except Exception as e:
            logger.error(f"发送消息失败: {str(e)}")
            return {"error": f"发送消息失败: {str(e)}"}
    
    def read_messages(self, topic_name: str, max_messages: int = 10, timeout_ms: int = 1000):
        try:
            # 检查Topic是否存在
            topics = self.get_topic_list()
            if topic_name not in topics:
                logger.warning(f"Topic {topic_name} 不存在")
                return [{"error": f"Topic {topic_name} 不存在"}]
            
            # 创建消费者
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                consumer_timeout_ms=timeout_ms,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )
            
            messages = []
            for i, message in enumerate(consumer):
                if i >= max_messages:
                    break
                
                messages.append({
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key.decode('utf-8') if message.key else None,
                    "value": message.value,
                    "timestamp": datetime.datetime.fromtimestamp(message.timestamp / 1000.0).isoformat()
                })
            
            consumer.close()
            return messages
        except Exception as e:
            logger.error(f"读取消息失败: {str(e)}")
            return [{"error": f"读取消息失败: {str(e)}"}]
    
    def replay_messages(self, topic_name: str, start_time: str, end_time: Optional[str] = None, max_messages: int = 100):
        try:
            # 检查Topic是否存在
            topics = self.get_topic_list()
            if topic_name not in topics:
                logger.warning(f"Topic {topic_name} 不存在")
                return [{"error": f"Topic {topic_name} 不存在"}]
            
            # 解析时间
            start_timestamp = int(parser.parse(start_time).timestamp() * 1000)
            end_timestamp = int(parser.parse(end_time).timestamp() * 1000) if end_time else int(datetime.datetime.now().timestamp() * 1000)
            
            # 创建消费者
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
            )
            
            # 获取Topic的所有分区
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                consumer.close()
                return [{"error": f"无法获取Topic {topic_name} 的分区信息"}]
            
            # 将所有分区分配给消费者
            topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
            consumer.assign(topic_partitions)
            
            # 对每个分区，找到对应时间戳的偏移量
            timestamps = {tp: start_timestamp for tp in topic_partitions}
            offsets = consumer.offsets_for_times(timestamps)
            
            # 设置每个分区的起始位置
            for tp, offset_and_timestamp in offsets.items():
                if offset_and_timestamp:  # 可能为None，如果没有消息在给定的时间戳之后
                    consumer.seek(tp, offset_and_timestamp.offset)
                else:
                    consumer.seek_to_end(tp)
            
            # 读取消息直到结束时间或达到最大消息数
            messages = []
            for i, message in enumerate(consumer):
                if i >= max_messages:
                    break
                
                # 检查消息时间戳是否在指定范围内
                if message.timestamp > end_timestamp:
                    break
                
                messages.append({
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "key": message.key.decode('utf-8') if message.key else None,
                    "value": message.value,
                    "timestamp": datetime.datetime.fromtimestamp(message.timestamp / 1000.0).isoformat()
                })
            
            consumer.close()
            return messages
        except Exception as e:
            logger.error(f"回放消息失败: {str(e)}")
            return [{"error": f"回放消息失败: {str(e)}"}]

    def close(self):
        if self.producer:
            self.producer.close()
        
        if self.admin_client:
            self.admin_client.close()
        
        logger.info("已关闭所有Kafka连接")

# 创建Kafka管理器实例
kafka_manager = KafkaManager()

@mcp.tool()
async def list_topics() -> List[str]:
    """
    列出Kafka中所有可用的Topic
    
    Returns:
        Topic名称列表
    """
    return kafka_manager.get_topic_list()

@mcp.tool()
async def get_topic_info(topic_name: str) -> Dict[str, Any]:
    """
    获取指定Topic的详细信息
    
    Args:
        topic_name: Topic名称
    
    Returns:
        包含Topic信息的字典，包括分区数量、每个分区的起始和结束偏移量以及消息数量
    """
    return kafka_manager.get_topic_info(topic_name)

@mcp.tool()
async def send_message(topic_name: str, message: str, key: Optional[str] = None) -> Dict[str, Any]:
    """
    向指定Topic发送消息
    
    Args:
        topic_name: Topic名称
        message: 要发送的消息内容，JSON字符串格式
        key: 消息的键（可选）
    
    Returns:
        包含发送结果的字典
    """
    try:
        message_data = json.loads(message)
    except json.JSONDecodeError:
        # 如果不是有效的JSON，则作为字符串发送
        message_data = message
    
    return kafka_manager.send_message(topic_name, message_data, key)

@mcp.tool()
async def read_messages(topic_name: str, max_messages: int = 10, timeout_ms: int = 1000) -> List[Dict[str, Any]]:
    """
    从指定Topic读取最新的消息
    
    Args:
        topic_name: Topic名称
        max_messages: 最大读取消息数，默认为10
        timeout_ms: 超时时间（毫秒），默认为1000
    
    Returns:
        消息列表
    """
    return kafka_manager.read_messages(topic_name, max_messages, timeout_ms)

@mcp.tool()
async def replay_messages(topic_name: str, start_time: str, end_time: Optional[str] = None, max_messages: int = 100) -> List[Dict[str, Any]]:
    """
    回放指定时间段内的消息
    
    Args:
        topic_name: Topic名称
        start_time: 开始时间，ISO格式的字符串，如'2023-01-01T00:00:00'
        end_time: 结束时间，ISO格式的字符串，默认为当前时间
        max_messages: 最大回放消息数，默认为100
    
    Returns:
        消息列表
    """
    return kafka_manager.replay_messages(topic_name, start_time, end_time, max_messages)

@mcp.resource()
async def kafka_connection_info() -> Dict[str, Any]:
    """
    获取当前Kafka连接信息
    
    Returns:
        包含Kafka连接信息的字典
    """
    return {
        "bootstrap_servers": kafka_manager.bootstrap_servers,
        "connection_status": "已连接" if kafka_manager.producer else "未连接"
    }

if __name__ == "__main__":
    mcp.run()