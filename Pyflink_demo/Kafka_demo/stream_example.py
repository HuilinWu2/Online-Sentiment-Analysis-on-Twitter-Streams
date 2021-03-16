import os
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf

kafka_servers = "localhost:9092"
kafka_consumer_group_id = "group0"  # group ID
source_topic = "stream_tweets"  # 源数据
sink_topic = "clean_stream"  # 结果

# ########################### 初始化流处理环境 ###########################
# 更多配置的设置请参考扩展阅读 1

# 创建 Blink 流处理环境，注意此处需要指定 StreamExecutionEnvironment，否则无法导入 java 函数
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
# 设置该参数以使用 UDF
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

# ########################### 指定 jar 依赖 ###########################

dir_kafka_sql_connect = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     'flink-sql-connector-kafka_2.11-1.12.0.jar')
t_env.get_config().get_configuration().set_string("pipeline.jars", 'file://' + dir_kafka_sql_connect)

# ########################### 指定 python 依赖 ###########################
# 可以在当前目录下看到 requirements.txt 依赖文件。
# 运行下述命令，成包含有安装包的 cached_dir 文件夹
# pip download -d cached_dir -r requirements.txt --no-binary :all:

dir_requirements = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'requirements.txt')
dir_cache = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'cached_dir')
if os.path.exists(dir_requirements):
    if os.path.exists(dir_cache):
        # 方式 1：指定包含依赖项的安装包的目录，它将被上传到集群以支持离线安装
        # 路径可以是绝对路径或相对路径，但注意路径前面不需要 file://
        t_env.set_python_requirements(dir_requirements, dir_cache)
    else:
        # 方式 2：指定描述依赖的依赖文件 requirements.txt，作业运行时下载，不推荐。
        t_env.set_python_requirements(dir_requirements)


# ########################### 注册 UDF ###########################

# class Model(ScalarFunction): # pylint: disable=abstract-class-instantiated
#     def __init__(self):
#         self.metric_counter = None  # 从作业开始至今的所有样本数量
#         self.metric_total_10_sec = None
        
#     def open(self, function_context):
#         """
#         访问指标系统，并注册指标，以便于在 webui (localhost:8081) 实时查看算法的运行情况。
#         :param function_context:
#         :return:
#         """
#         # 访问指标系统，并定义 Metric Group 名称为 online_ml 以便于在 webui 查找
#         # Metric Group + Metric Name 是 Metric 的唯一标识
#         metric_group = function_context.get_metric_group().add_group("online_ml")

#         # 目前 PyFlink 1.11.2 版本支持 4 种指标：计数器 Counters，量表 Gauges，分布 Distribution 和仪表 Meters 。
#         # 目前这些指标都只能是整数

#         # 1、计数器 Counter，用于计算某个东西出现的次数，可以通过 inc()/inc(n:int) 或 dec()/dec(n:int) 来增加或减少值
#         self.metric_counter = metric_group.counter('sample_count')  # 训练过的样本数量
#         self.metric_total_10_sec = metric_group.meter("total_10_sec", time_span_in_seconds=10)

#     def eval(self, text,label):
#         import re
#         text = re.sub(r'\W', ' ', text[0])
#         text = re.sub(r'^br$', ' ', text)
#         text = re.sub(r'\s+^br$\s+', ' ', text)
#         text = re.sub(r'\s+[a-z]\s+', ' ', text)
#         text = re.sub(r'^b\s+', ' ', text)
#         text = re.sub(r'\s+', ' ', text) 
#         text = text.lower().strip()
#         # 更新指标
#         self.metric_counter.inc(1)  # 训练过的样本数量 + 1
#         self.metric_total_10_sec.mark_event(1)  # 更新仪表 Meter ：来一条数据就 + 1 ，统计 10 秒内的样本量
#         # 返回结果
#         return 3


# clean = udf(Model(), input_types=[DataTypes.STRING(), DataTypes.INT()], 
#             result_type=DataTypes.STRING()) # pylint: disable=abstract-class-instantiated
# t_env.register_function('text_clean', clean)

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def process(text):
    import re
    text = re.sub(r'\W', ' ', text[0])
    text = re.sub(r'^br$', ' ', text)
    text = re.sub(r'\s+^br$\s+', ' ', text)
    text = re.sub(r'\s+[a-z]\s+', ' ', text)
    text = re.sub(r'^b\s+', ' ', text)
    text = re.sub(r'\s+', ' ', text) 
    text = text.lower().strip()
    return text
t_env.register_function('process', process) 


# ########################### 创建源表(source) ###########################
# 使用 Kafka-SQL 连接器从 Kafka 实时消费数据。

t_env.execute_sql(f"""
CREATE TABLE source (
    x STRING,            -- 图片灰度数据
    actual_y INT,            -- 实际数字
    ts TIMESTAMP(3)              -- 图片产生时间
) with (
    'connector' = 'kafka',
    'topic' = '{source_topic}',
    'properties.bootstrap.servers' = '{kafka_servers}',
    'properties.group.id' = '{kafka_consumer_group_id}',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'format' = 'json'
)
""")

# ########################### 创建结果表(sink) ###########################
# 将统计结果实时写入到 Kafka

t_env.execute_sql(f"""
CREATE TABLE sink (             
    clean STRING
) with (
    'connector' = 'kafka',
    'topic' = '{sink_topic}',
    'properties.bootstrap.servers' = '{kafka_servers}',
    'properties.group.id' = '{kafka_consumer_group_id}',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'format' = 'json'
)
""")

# ########################### 流处理任务 ###########################
# actual_y AS label text_clean(x,actual_y)
# 在线学习
t_env.sql_query("""
SELECT
    process(x) AS clean
FROM
    source
""").insert_into("sink")
t_env.execute('tweets_cleaning')