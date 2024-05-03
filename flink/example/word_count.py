from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

def get_flat_map(lines):
    r = lines.split(' ')
    yield from r


def word_count_batch_demo():
    """
    DataStream 实现 WordCount 有界流（读文件）
    :return:
    """
    # 1、创建执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # 2、从文件中读取数据
    file_name = './word.txt'
    data_source = env.read_text_file(file_path=file_name)

    # 3、数据处理
    ds = data_source\
        .flat_map(get_flat_map)\
        .map(lambda x: (x, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))\
        .key_by(lambda x: x[0]).sum(1)
    # 4、输出
    ds.print()
    # 5、执行
    env.execute()


if __name__ == '__main__':
    word_count_batch_demo()
