# coding=utf8
"""
!!! 注意：用于发布时使用， 发布日志：1、不需要显示print的内容。 2、日志格式，不显示模块与日志所在的代码行数。 !!!
"""
import sys
import logging
import os
from pathlib import Path
import socket
from pythonjsonlogger.jsonlogger import JsonFormatter

Q_LOG = "v0.1.1"

PRINT_WRTIE_FILE_NAME = os.environ.get("PRINT_WRTIE_FILE_NAME") or Path(sys.path[1]).name + '.print'  # None 不开启pint日志记录
SYS_STD_FILE_NAME = None  # 不开启sprint日志记录

USE_BULK_STDOUT_ON_WINDOWS = False  # 在win上是否每隔0.1秒批量stdout,win的io太差了

DEFAULUT_USE_COLOR_HANDLER = True  # 是否默认使用有彩的日志。
DEFAULUT_IS_USE_LOGURU_STREAM_HANDLER = False  # 是否默认使用 loguru的控制台日志
DISPLAY_BACKGROUD_COLOR_IN_CONSOLE = False  # 在控制台是否显示彩色块状的日志
AUTO_PATCH_PRINT = True

SHOW_PYCHARM_COLOR_SETINGS = False  # 是否提示优化pycahrm控制台颜色
SHOW_NB_LOG_LOGO = False  # 是否启动打印nb_log的logo图形
SHOW_IMPORT_NB_LOG_CONFIG_PATH = False  # 是否打印读取的nb_log_config.py的文件位置

WHITE_COLOR_CODE = 37

DEFAULT_ADD_MULTIPROCESSING_SAFE_ROATING_FILE_HANDLER = True  # 是否默认同时将日志记录到记log文件记事本中，就是用户不指定 log_filename的值，会自动写入日志命名空间.log文件中。
AUTO_WRITE_ERROR_LEVEL_TO_SEPARATE_FILE = False
LOG_FILE_SIZE = 1000
LOG_FILE_BACKUP_COUNT = 10

LOG_PATH = os.getenv("LOG_PATH")
if not LOG_PATH:
    LOG_PATH = './pythonlogs'
    if os.name == 'posix':
        home_path = os.environ.get("HOME", '/')
        LOG_PATH = Path('./pythonlogs')
# print('LOG_PATH:', LOG_PATH)

LOG_FILE_HANDLER_TYPE = 6
"""
6 BothDayAndSizeRotatingFileHandler 使用本人完全彻底开发的，同时按照时间和大小切割，无论是文件的大小、还是时间达到了需要切割的条件就切割。
"""

LOG_LEVEL_FILTER = logging.DEBUG
FILTER_WORDS_PRINT = []  # 不打印的字符


def get_host_ip():
    ip = ''
    host_name = ''
    # noinspection PyBroadException
    try:
        sc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sc.connect(('8.8.8.8', 80))
        ip = sc.getsockname()[0]
        host_name = socket.gethostname()
        sc.close()
    except Exception:
        pass
    return ip, host_name


computer_ip, computer_name = get_host_ip()


class JsonFormatterJumpAble(JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        log_record[f"{record.__dict__.get('pathname')}:{record.__dict__.get('lineno')}"] = ''  # 加个能点击跳转的字段。
        log_record['ip'] = computer_ip
        log_record['host_name'] = computer_name
        super().add_fields(log_record, record, message_dict)
        if 'for_segmentation_color' in log_record:
            del log_record['for_segmentation_color']


DING_TALK_TOKEN = '3dd0eexxxxxadab014bd604XXXXXXXXXXXX'  # 钉钉报警机器人

EMAIL_HOST = ('smtp.sohu.com', 465)
EMAIL_FROMADDR = 'aaa0509@sohu.com'  # 'matafyhotel-techl@matafy.com',
EMAIL_TOADDRS = ('cccc.cheng@silknets.com', 'yan@dingtalk.com',)
EMAIL_CREDENTIALS = ('aaa0509@sohu.com', 'abcdefg')

ELASTIC_HOST = '127.0.0.1'
ELASTIC_PORT = 9200

KAFKA_BOOTSTRAP_SERVERS = ['192.168.199.202:9092']
ALWAYS_ADD_KAFKA_HANDLER_IN_TEST_ENVIRONENT = False

MONGO_URL = 'mongodb://myUserAdmin:mimamiama@127.0.0.1:27016/admin'

RUN_ENV = 'test'

FORMATTER_DICT = {
    1: logging.Formatter(
        '日志时间【%(asctime)s】 - 日志名称【%(name)s】 - 文件【%(filename)s】 - 第【%(lineno)d】行 - 日志等级【%(levelname)s】 - 日志信息【%(message)s】',
        "%Y-%m-%d %H:%M:%S"),
    2: logging.Formatter(
        '%(asctime)s - %(name)s - %(filename)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s',
        "%Y-%m-%d %H:%M:%S"),
    3: logging.Formatter(
        '%(asctime)s - %(name)s - 【 File "%(pathname)s", line %(lineno)d, in %(funcName)s 】 - %(levelname)s - %(message)s',
        "%Y-%m-%d %H:%M:%S"),  # 一个模仿traceback异常的可跳转到打印日志地方的模板
    4: logging.Formatter(
        '%(asctime)s - %(name)s - "%(filename)s" - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s -               File "%(pathname)s", line %(lineno)d ',
        "%Y-%m-%d %H:%M:%S"),  # 这个也支持日志跳转
    5: logging.Formatter(
        '%(asctime)s.%(msecs)03d | %(levelname)s | p%(process)d_t%(thread)d | "%(pathname)s:%(lineno)d" - %(funcName)s - %(message)s',
        "%Y-%m-%d %H:%M:%S"),  # 我认为的最好的模板,推荐
    15: logging.Formatter(
        '%(asctime)s.%(msecs)03d | %(levelname)s | p%(process)d_t%(thread)d | "%(filename)s:%(lineno)d" - %(message)s',
        "%Y-%m-%d %H:%M:%S"),  # 简洁模板
    6: logging.Formatter('%(name)s - %(asctime)-15s - %(filename)s - %(lineno)d - %(levelname)s: %(message)s',
                         "%Y-%m-%d %H:%M:%S"),
    7: logging.Formatter('%(asctime)s - %(name)s - "%(filename)s:%(lineno)d" - %(levelname)s - %(message)s',
                         "%Y-%m-%d %H:%M:%S"),  # 一个只显示简短文件名和所处行数的日志模板

    8: JsonFormatterJumpAble(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(filename)s %(lineno)d  %(process)d %(thread)d',
        "%Y-%m-%d %H:%M:%S.%f",
        json_ensure_ascii=False),  # 这个是json日志，方便elk采集分析.

    9: logging.Formatter(
        '[p%(process)d_t%(thread)d] %(asctime)s - %(name)s - "%(pathname)s:%(lineno)d" - %(funcName)s - %(levelname)s - %(message)s',
        "%Y-%m-%d %H:%M:%S"),  # 对5改进，带进程和线程显示的日志模板。
    10: logging.Formatter(
        '[p%(process)d_t%(thread)d] %(asctime)s - %(name)s - "%(filename)s:%(lineno)d" - %(levelname)s - %(message)s',
        "%Y-%m-%d %H:%M:%S"),  # 对7改进，带进程和线程显示的日志模板。
    11: logging.Formatter(
        f'%(asctime)s-({computer_ip},{computer_name})-[p%(process)d_t%(thread)d] - %(name)s - "%(filename)s:%(lineno)d" - %(funcName)s - %(levelname)s - %(message)s',
        "%Y-%m-%d %H:%M:%S"),  # 对7改进，带进程和线程显示的日志模板以及ip和主机名。
}

FORMATTER_KIND = 5  # 如果get_logger不指定日志模板，则默认选择第几个模板
