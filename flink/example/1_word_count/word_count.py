import os
import shutil
from pyflink.table import BatchTableEnvironment, EnvironmentSettings
from pyflink.table import DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = BatchTableEnvironment.create(environment_settings=env_settings)
dir_word = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'word.csv')
dir_result = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'result')
# 如果文件/文件夹存在，则删除
if os.path.exists(dir_result):
    if os.path.isfile(dir_result):
        os.remove(dir_result)
    else:
        shutil.rmtree(dir_result, True)

t_env.execute_sql(f"""
    CREATE TABLE source (
        id BIGINT,     -- ID
        word STRING    -- 单词
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{dir_word}',
        'format' = 'csv'
    )
""")

t_env.execute_sql(f"""
    CREATE TABLE sink (
        word STRING,   -- 单词
        cnt BIGINT     -- 出现次数
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{dir_result}',
        'format' = 'csv'
    )
""")

t_env.sql_query("""
    SELECT word
           , count(1) AS cnt
    FROM source
    GROUP BY word
""").insert_into('sink')
t_env.execute('t')


