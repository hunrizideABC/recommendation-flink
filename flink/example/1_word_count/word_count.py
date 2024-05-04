from pyflink.table import EnvironmentSettings, TableEnvironment

environment_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
t_env = TableEnvironment.create(environment_settings=environment_settings)
t_env.get_config().get_configuration().set_string('parallelism.default', '1')

t_env.execute_sql("""
         CREATE TABLE testSource (
           word STRING
         ) WITH (
           'connector' = 'filesystem',
           'format' = 'csv',
           'path' = '/opt/flink/recommendation-flink/flink/example/1_word_count/input_file'
         )
     """)

t_env.execute_sql("""
         CREATE TABLE testSink (
           word STRING,
           `count` BIGINT
         ) WITH (
           'connector' = 'filesystem',
           'format' = 'csv',
           'path' = '/opt/flink/recommendation-flink/flink/example/1_word_count/output_file'
         )
     """)

t_env.from_path('testSource') \
    .group_by('word') \
    .select('word, count(1)') \
    .insert_into('testSink')

# Execute
t_env.execute("1-word_count")




