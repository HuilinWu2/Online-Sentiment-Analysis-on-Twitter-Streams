import os
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf

kafka_servers = "localhost:9092"
kafka_consumer_group_id = "group0" 
source_topic = "stream_tweets" 
sink_topic = "clean_stream"  

# BlinkStreamExecutionEnvironment that capable for Java function
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
# UDF configuration
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

# kafka dependency
dir_kafka_sql_connect = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     'flink-sql-connector-kafka_2.11-1.12.0.jar')
t_env.get_config().get_configuration().set_string("pipeline.jars", 'file://' + dir_kafka_sql_connect)

# python dependency
requirements = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'requirements.txt')
dir_cache = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'cached_dir')
if os.path.exists(requirements):
    if os.path.exists(dir_cache):
        t_env.set_python_requirements(requirements, dir_cache)
    else:
        t_env.set_python_requirements(requirements)

# UDF
# class Model(ScalarFunction): # pylint: disable=abstract-class-instantiated
#     def __init__(self):
#         self.metric_counter = None 
#         self.metric_total_10_sec = None
        
#     def open(self, function_context):
#         metric_group = function_context.get_metric_group().add_group("online_ml")
#         self.metric_counter = metric_group.counter('sample_count')
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

#         self.metric_counter.inc(1)
#         self.metric_total_10_sec.mark_event(1) 
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


# ============= create source ==========================
t_env.execute_sql(f"""
CREATE TABLE source (
    x STRING,         
    y INT,       
    ts TIMESTAMP(3)          
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

# ============= create sink ==========================
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

# job submission
t_env.sql_query("""
SELECT
    process(x) AS clean
FROM
    source
""").insert_into("sink")
t_env.execute('tweets_cleaning')
