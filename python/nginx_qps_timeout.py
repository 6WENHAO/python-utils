#!/usr/bin/python
#coding=utf-8
import ast
import json
import traceback
import logging
import sys, os
import socket
import time
import datetime

LOG_DIR = "/jboss/openresty/nginx/logs/"  # 设置日志目录路径
# LOG_DIR = "/worklight/openresty/nginx/logs/"  # 设置日志目录路径
FILE_POS_FILE = '/tmp/nginx_process_uri_qps_file_pos.txt'
FILE_TIME_FILE = '/tmp/nginx_process_uri_qps_use_time.txt'
# LOG_DIR = "/Users/wenhao/Desktop/logs/"  # 设置日志目录路径
# FILE_POS_FILE = '/Users/wenhao/Desktop/logs/tmp/nginx_process_uri_qps_file_pos.txt'
# FILE_TIME_FILE = '/Users/wenhao/Desktop/logs/tmp/nginx_process_uri_qps_use_time.txt'

LINE_SEPARATOR = '\n'  # 行分隔符
FIELD_SEPARATOR = '|;'  # 字段分隔符
TIME_FIELD_POS = 0  # 时间字段位置
UPSTREAM_TIME_FIELD_POS = 4  # 后端响应时间字段位置
RESPONSE_TIME_FIELD_POS = 5  # 前端响应时间字段位置
ENGINE_FIELD_POS = 6  # ENGIN字段位置
REDIS_FIELD_POS = 7  # REDIS字段位置
REDIS_USERMAPPER_POS = 8  # USERMAPPER字段位置
REDIS_REALTIME_POS = 9  # REALTIMEINFO字段位置

READ_CHUNK = 1024 * 1024 * 10  # 每次读取10MB
INTERVAL = 60  # 循环间隔60秒

LIMIT_SEC = 600  # 统计最近10分钟,超过的时间丢弃

GLOBAL_REQS = {}  # 全局请求数
GLOBAL_URIS = {}  # 全局URI数
GLOBAL_TIMEDIFFS = {}  # 全局时间差
GLOBAL_TIMEDIFFS_AVG = {}  # 全局时间差平均
GLOBAL_ENGINE_TIME = {}  # 全局引擎时间
GLOBAL_ENGINE_SUCCESS = {}  # 全局引擎成功率
GLOBAL_ENGINE_QPS = {}  # 全局引擎成功率
GLOBAL_REDIS_TIME = {}  # 全局REDIS时间
GLOBAL_REDIS_SUCCESS = {}  # 全局REDIS成功率
GLOBAL_REDIS_QPS = {}  # 全局REDIS成功率
GLOBAL_USERMAPPER_TIME = {}  # 全局USERMAPPER时间
GLOBAL_USERMAPPER_SUCCESS = {}  # 全局USERMAPPER成功率
GLOBAL_USERMAPPER_QPS = {}  # 全局USERMAPPER的QPS
GLOBAL_REALTIME_TIME = {}  # 全局REALTIME时间
GLOBAL_REALTIME_SUCCESS = {}  # 全局REALTIME成功率
GLOBAL_REALTIME_QPS = {}  # 全局REALTIME的QPS

SAVE_MINUTE_BEG = 10  # 存储从 前10分钟开始
SAVE_MINUTE_END = 1  # 存储从 前5分钟结束

G_LOGGER = None


def setup_logger(log_file):
    """
    配置 logger 以将日志输出到指定文件。

    :param log_file: 日志文件的路径
    """
    global G_LOGGER

    # 创建 logger 对象
    G_LOGGER = logging.getLogger('my_logger')
    G_LOGGER.setLevel(logging.DEBUG)  # 设置最低日志级别为 DEBUG

    # 创建文件处理器并设置日志格式
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # 将处理器添加到 logger
    G_LOGGER.addHandler(file_handler)


def close_logger():
    G_LOGGER.setLevel(logging.CRITICAL)  # 设置日志级别为 CRITICAL，禁用所有日志记录


def log_message(message):
    """
    使用全局 logger 记录一条消息。

    :param message: 要记录的消息字符串
    """
    global G_LOGGER
    G_LOGGER.info(message)


def get_bucket(time_diff_ms):
    for bucket_name, (lower, upper) in TIME_BUCKETS.items():
        if lower <= time_diff_ms < upper:
            return bucket_name
    return None


# 输出统计结果到控制台和文件
def output_timediffs(save_timediffs, local_ip):
    sorted_items = sorted(save_timediffs.items(), key=lambda item: item[0])
    metric = 'edge.timediff_qps'
    for timestamp, timediffs_dict in sorted_items:
        for bucket, count in timediffs_dict.items():
            print("%s %d %d ip=%s bucket=%s" % (metric, timestamp, count, local_ip, bucket))
            log_message("%s %d %d ip=%s bucket=%s" % (metric, timestamp, count, local_ip, bucket))


# 输出统计结果到控制台和文件
def output_reqs(save_reqs, local_ip):
    sorted_items = sorted(save_reqs.items(), key=lambda item: item[0])
    metric = 'edge.req_qps'
    for timestamp, count in sorted_items:
        print("%s %d %d ip=%s" % (metric, timestamp, count, local_ip))
        log_message("%s %d %d ip=%s" % (metric, timestamp, count, local_ip))


# 输出统计结果到控制台和文件
def output_timediffs_by_name(save_timediffs, local_ip, metric):
    sorted_items = sorted(save_timediffs.items(), key=lambda item: item[0])
    for timestamp, timediffs_dict in sorted_items:
        for bucket, count in timediffs_dict.items():
            print("%s %d %d ip=%s bucket=%s" % (metric, timestamp, count, local_ip, bucket))
            log_message("%s %d %d ip=%s bucket=%s" % (metric, timestamp, count, local_ip, bucket))


# 输出统计结果到控制台和文件
def output_reqs_by_name(save_reqs, local_ip, metric):
    sorted_items = sorted(save_reqs.items(), key=lambda item: item[0])
    for timestamp, count in sorted_items:
        print("%s %d %d ip=%s" % (metric, timestamp, count, local_ip))
        log_message("%s %d %d ip=%s" % (metric, timestamp, count, local_ip))


def process_global_timedict(in_dict, beg_min, end_min):
    """
    处理 in_dict 字典，返回指定时间范围内的记录，并删除不在范围内的记录。

    :param beg_min: 开始分钟数，表示比当前时间早多少分钟
    :param end_min: 结束分钟数，表示比当前时间早多少分钟
    :return: 在 beg_min 和 end_min 范围内的记录
    """
    # 获取当前时间
    current_time = datetime.datetime.now()
    # 获取当前秒数
    current_seconds = current_time.second

    current_time = current_time - datetime.timedelta(seconds=current_seconds)

    # 计算 beg_min 和 end_min 对应的时间点
    beg_time = current_time - datetime.timedelta(minutes=beg_min)
    end_time = current_time - datetime.timedelta(minutes=end_min)

    # 存储在范围内的记录
    in_range_records = {}

    # 存储需要删除的时间戳
    to_delete = []

    # 遍历 GLOBAL_URIS
    for timestamp_str, val in in_dict.items():
        # 将时间戳字符串转换为 datetime 对象
        try:
            # 尝试将时间戳解析为秒级时间戳
            timestamp = datetime.datetime.fromtimestamp(int(timestamp_str))
        except ValueError:
            log_message("Error parsing timestamp: %s" % timestamp_str)
            continue

        if beg_time < timestamp < end_time:
            # 时间戳在 beg_min 和 end_min 范围内
            in_range_records[timestamp_str] = val
            to_delete.append(timestamp_str)
        elif timestamp < beg_time:
            # 时间戳比 beg_min 更早
            to_delete.append(timestamp_str)

    # 删除不在范围内的记录
    for ts in to_delete:
        del in_dict[ts]

    return in_range_records


def prase_timestamp(input_timestamp, limit_sec):
    """
    处理输入的时间戳字符串，并根据给定的条件返回相应的 Linux 时间戳。

    :param input_timestamp: 字符串时间戳，格式为 YYYYMMDDHHMMSSsss (例如 '20250312141918680')
    :param limit_sec: 时间截至的秒数，超过这个时间的记录将被丢弃
    :param beg_min: 开始分钟数
    :param end_min: 结束分钟数
    :return: 如果满足条件，
    :        第一个返回值 返回对应的 Linux 时间戳(分钟或秒）；否则返回 None
    :        第二个返回值 返回Linux 秒时间戳
    """
    # 解析输入的时间戳字符串
    try:
        dt = datetime.datetime.strptime(input_timestamp, '%Y%m%d%H%M%S%f')
    except ValueError as e:
        log_message("Error parsing timestamp: %s" % input_timestamp)
        return None, None

    # 获取当前时间
    current_time = datetime.datetime.now()

    # 计算时间截至点
    limit_time = current_time - datetime.timedelta(seconds=limit_sec)

    # 检查时间戳是否在 limit_sec 时间之前
    if dt < limit_time:
        # log_message("Timestamp is older than limit_sec. %s" % (input_timestamp))
        return None, None

    second_timestamp = int(time.mktime(dt.timetuple()))

    # 转换为秒级时间戳
    return second_timestamp, second_timestamp


# 获取本机IPv4地址，优先获取内网IP
def get_local_ip():
    for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
        if not ip.startswith("127.") and ip.count('.') == 3:
            return ip
    return "127.0.0.1"


# 判断是否为合法的时间戳
def is_valid_time(timestamp):
    try:
        datetime.datetime.strptime(str(timestamp), '%Y%m%d%H%M%S%f')
        return True
    except ValueError:
        return False


# 根据当前时间生成日志文件名
def generate_log_filename(logdir, start_time):
    dayhour = time.strftime("%Y%m%d%H", time.localtime(start_time))
    one_hour_ago_timestamp = start_time - 3600
    last_one_hour = time.strftime("%Y%m%d%H", time.localtime(one_hour_ago_timestamp))
    two_hour_ago_timestamp = start_time - 7200
    last_two_hour = time.strftime("%Y%m%d%H", time.localtime(two_hour_ago_timestamp))
    return os.path.join(logdir, "process.trace." + dayhour + ".log"), os.path.join(logdir,
                                                                                   "process.trace." + last_one_hour + ".log"), os.path.join(
        logdir, "process.trace." + last_two_hour + ".log")


# 读取 file_pos文件并返回旧文件和新文件的文件名及读取位置
def read_pos_file(in_file):
    last_filename = ""
    last_read_pos = 0
    now_filename = ""
    now_read_pos = 0

    if os.path.exists(in_file):
        with open(in_file, 'r') as f:
            lines = f.readlines()
            if len(lines) >= 2:
                # 解析旧文件信息
                last_line = lines[0].strip()
                parts = last_line.split(':')
                if len(parts) == 2:
                    last_filename = parts[0]
                    last_read_pos = int(parts[1])

                # 解析新文件信息
                now_line = lines[1].strip()
                parts = now_line.split(':')
                if len(parts) == 2:
                    now_filename = parts[0]
                    now_read_pos = int(parts[1])

    return last_filename, last_read_pos, now_filename, now_read_pos


# 输出统计结果到控制台和文件
def output_stat(start_time, local_ip):
    with open(FILE_TIME_FILE, 'a') as f_out:
        f_out.write('%s | %s | %.2f | %d | %d \n' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
                                                     time.strftime('%Y-%m-%d %H:%M:%S'), time.time() - start_time, 0,
                                                     0))


# 写入 file_pos 文件
def write_pos_file(out_file, last_filename, last_read_pos, now_filename, now_read_pos):
    with open(out_file, 'w') as f_inode:
        f_inode.write('%s:%d\n' % (last_filename, last_read_pos))
        f_inode.write('%s:%d\n' % (now_filename, now_read_pos))


def process_timeout_fields(fields):
    """
    处理 fields 列表，计算 response_time 和 upstream_time 的差值，并将其转换为毫秒。

    :param fields: 包含时间字段的列表
    :return: response_time 和 upstream_time 的差值（毫秒）
    """
    if len(fields) > UPSTREAM_TIME_FIELD_POS and len(fields) > RESPONSE_TIME_FIELD_POS:
        try:
            upstream_time = float(fields[UPSTREAM_TIME_FIELD_POS])
            response_time = float(fields[RESPONSE_TIME_FIELD_POS])
            # 计算差值
            time_diff_seconds = response_time - upstream_time
            # 将差值转换为毫秒
            time_diff_milliseconds = int(time_diff_seconds * 1000)
            return time_diff_milliseconds
        except ValueError as e:
            log_message("Error parsing time fields: %s" % str(e))
            return None
    else:
        log_message("Fields list does not contain enough elements.")
        return None


def process_log_chunk(chunk, read_pos):
    lines = chunk.split(LINE_SEPARATOR.encode('utf-8'))
    # 更新 read_pos
    if len(lines) > 1:
        read_pos += len(chunk) - len(lines[-1])
        lines[-1] = b''  # 最后一个元素可能是未完成的记录
    else:
        read_pos += len(chunk)

    for line in lines[:-1]:
        # 将字节字符串解码为普通字符串
        line_str = line.decode('utf-8', 'replace')
        fields = line_str.split(FIELD_SEPARATOR)
        if len(fields) < 7 or not is_valid_time(fields[TIME_FIELD_POS]):
            continue

        line_timestamp, second_timestamp = prase_timestamp(fields[TIME_FIELD_POS], LIMIT_SEC)
        if line_timestamp is None:
            continue

        # 处理 request
        if second_timestamp not in GLOBAL_REQS:
            GLOBAL_REQS[second_timestamp] = 0
        GLOBAL_REQS[second_timestamp] += 1

        # 处理 response time
        time_diff_ms = process_timeout_fields(fields)
        if time_diff_ms is not None:
            bucket = get_bucket(time_diff_ms)
            if bucket:
                if second_timestamp not in GLOBAL_TIMEDIFFS:
                    GLOBAL_TIMEDIFFS[second_timestamp] = {}
                if bucket not in GLOBAL_TIMEDIFFS[second_timestamp]:
                    GLOBAL_TIMEDIFFS[second_timestamp][bucket] = 0
                GLOBAL_TIMEDIFFS[second_timestamp][bucket] += 1

        # 处理 response time avg
        if time_diff_ms is not None:
            if second_timestamp not in GLOBAL_TIMEDIFFS_AVG:
                GLOBAL_TIMEDIFFS_AVG[second_timestamp] = []
            GLOBAL_TIMEDIFFS_AVG[second_timestamp].append(time_diff_ms)


        # # 处理 引擎 time
        # engine_field = fields[ENGINE_FIELD_POS]
        # engine_time = get_arr_time(engine_field)
        # if engine_time is not None:
        #     bucket = get_bucket(engine_time)
        #     if bucket:
        #         if second_timestamp not in GLOBAL_ENGINE_TIME:
        #             GLOBAL_ENGINE_TIME[second_timestamp] = {}
        #         if bucket not in GLOBAL_ENGINE_TIME[second_timestamp]:
        #             GLOBAL_ENGINE_TIME[second_timestamp][bucket] = 0
        #         GLOBAL_ENGINE_TIME[second_timestamp][bucket] += 1

        # 处理 引擎 time avg
        engine_field = json.loads(fields[ENGINE_FIELD_POS])
        engine_time = get_arr_time_avg(engine_field)
        if second_timestamp not in GLOBAL_ENGINE_TIME:
            GLOBAL_ENGINE_TIME[second_timestamp] = []
        if engine_time is not None:
            GLOBAL_ENGINE_TIME[second_timestamp].append(engine_time)

        # 处理 引擎 success
        if second_timestamp not in GLOBAL_ENGINE_SUCCESS:
            GLOBAL_ENGINE_SUCCESS[second_timestamp] = [0, 0]
        engine_success = get_engine_success(engine_field, GLOBAL_ENGINE_SUCCESS[second_timestamp])
        GLOBAL_ENGINE_SUCCESS[second_timestamp] = engine_success

        # 处理 引擎 qps
        calculate_qps(engine_field, second_timestamp, GLOBAL_ENGINE_QPS)

        # 处理 redis time
        # redis_field = fields[REDIS_FIELD_POS]
        # redis_time = get_arr_time(redis_field)
        # if redis_time is not None:
        #     bucket = get_bucket(redis_time)
        #     if bucket:
        #         if second_timestamp not in GLOBAL_REDIS_TIME:
        #             GLOBAL_REDIS_TIME[second_timestamp] = {}
        #         if bucket not in GLOBAL_REDIS_TIME[second_timestamp]:
        #             GLOBAL_REDIS_TIME[second_timestamp][bucket] = 0
        #         GLOBAL_REDIS_TIME[second_timestamp][bucket] += 1

        # 处理 redis time avg
        redis_field = json.loads(fields[REDIS_FIELD_POS])
        redis_time = get_arr_time_avg(redis_field)
        if second_timestamp not in GLOBAL_REDIS_TIME:
            GLOBAL_REDIS_TIME[second_timestamp] = []
        if redis_time is not None:
            GLOBAL_REDIS_TIME[second_timestamp].append(redis_time)

        # 处理 redis success
        if second_timestamp not in GLOBAL_REDIS_SUCCESS:
            GLOBAL_REDIS_SUCCESS[second_timestamp] = [0, 0]
        redis_success = arr_success(redis_field, GLOBAL_REDIS_SUCCESS[second_timestamp])
        GLOBAL_REDIS_SUCCESS[second_timestamp] = redis_success

        # 处理 引擎 qps
        calculate_qps(redis_field, second_timestamp, GLOBAL_REDIS_QPS)

        # 处理 usermapping time
        # usermapping_field = fields[REDIS_USERMAPPER_POS]
        # usermapping_time = get_arr_time(usermapping_field)
        # if usermapping_time is not None:
        #     bucket = get_bucket(usermapping_time)
        #     if bucket:
        #         if second_timestamp not in GLOBAL_USERMAPPER_TIME:
        #             GLOBAL_USERMAPPER_TIME[second_timestamp] = {}
        #         if bucket not in GLOBAL_USERMAPPER_TIME[second_timestamp]:
        #             GLOBAL_USERMAPPER_TIME[second_timestamp][bucket] = 0
        #         GLOBAL_USERMAPPER_TIME[second_timestamp][bucket] += 1

        # 处理 usermapping time avg
        usermapping_field = json.loads(fields[REDIS_USERMAPPER_POS])
        usermapping_time = get_arr_time_avg(usermapping_field)
        if second_timestamp not in GLOBAL_USERMAPPER_TIME:
            GLOBAL_USERMAPPER_TIME[second_timestamp] = []
        if usermapping_time is not None:
            GLOBAL_USERMAPPER_TIME[second_timestamp].append(usermapping_time)

        # 处理 usermapping qps
        calculate_qps(usermapping_field, second_timestamp, GLOBAL_USERMAPPER_QPS)

        # 处理 usermapping success
        if second_timestamp not in GLOBAL_USERMAPPER_SUCCESS:
            GLOBAL_USERMAPPER_SUCCESS[second_timestamp] = [0, 0]
        usermapping_success = arr_success(usermapping_field, GLOBAL_USERMAPPER_SUCCESS[second_timestamp])
        GLOBAL_USERMAPPER_SUCCESS[second_timestamp] = usermapping_success

        # 处理 realtime time
        # realtime_field = fields[GLOBAL_REALTIME_TIME]
        # realtime_time = get_arr_time(realtime_field)
        # if realtime_time is not None:
        #     bucket = get_bucket(realtime_time)
        #     if bucket:
        #         if second_timestamp not in GLOBAL_REALTIME_TIME:
        #             GLOBAL_REALTIME_TIME[second_timestamp] = {}
        #         if bucket not in GLOBAL_REALTIME_TIME[second_timestamp]:
        #             GLOBAL_REALTIME_TIME[second_timestamp][bucket] = 0
        #         GLOBAL_REALTIME_TIME[second_timestamp][bucket] += 1

        # 处理 realtime time avg
        realtime_field = json.loads(fields[REDIS_REALTIME_POS])
        realtime_time = get_arr_time_avg(realtime_field)
        if second_timestamp not in GLOBAL_REALTIME_TIME:
            GLOBAL_REALTIME_TIME[second_timestamp] = []
        if realtime_time is not None:
            GLOBAL_REALTIME_TIME[second_timestamp].append(realtime_time)

        # 处理 realtime success
        if second_timestamp not in GLOBAL_REALTIME_SUCCESS:
            GLOBAL_REALTIME_SUCCESS[second_timestamp] = [0, 0]
        realtime_success = arr_success(realtime_field, GLOBAL_REALTIME_SUCCESS[second_timestamp])
        GLOBAL_REALTIME_SUCCESS[second_timestamp] = realtime_success

        # 处理 realtime qps
        calculate_qps(realtime_field, second_timestamp, GLOBAL_REALTIME_QPS)


    return read_pos


def calculate_qps(field, second_timestamp,global_arr):
    if second_timestamp not in global_arr:
        global_arr[second_timestamp] = 0
    engine_count = get_arr_qps(field)
    global_arr[second_timestamp] += engine_count


# 获取数组中的第一个时间参数
def get_arr_time(arr):
    if len(arr) > 3:
        try:
            arr_time = float(arr[0])
            return arr_time
        except ValueError as e:
            log_message("Error parsing arr fields: %s" % str(e))
            return None
    else:
        log_message("Fields list does not contain enough elements.")
        return None


# 获取数组中的第一个时间参数平均值
def get_arr_time_avg(arr):
    if len(arr) > 2:
        try:
            arr_time = int(float(arr[0]) * 1000)
            return arr_time
        except ValueError as e:
            log_message("Error parsing arr fields: %s" % str(e))
            return None
    else:
        # log_message("Fields list does not contain enough elements.")
        return None


# 获取时间平均值
def get_time_avg(global_arr):
    for second_timestamp, time_diffs in global_arr.items():
        if time_diffs:
            average_time_diff = sum(time_diffs) / len(time_diffs)
            global_arr[second_timestamp] = average_time_diff
        else:
            global_arr[second_timestamp] = 0  # 如果列表为空，设置平均值为 0 或其他适当的值


# 处理日志文件块并更新统计信息
def get_engine_success(arr, arr2):
    """
    计算数组中第三个数除以第二个数的百分比

    :param arr2: 按秒缓存请求数与成功数
    :param arr: 输入数组（列表）
    :return: 百分比（浮点数），如果出错则返回 None
    """
    # 确保数组至少有3个元素
    if len(arr) < 3:
        return [arr2[0], arr2[1]]
    # 获取第二个和第三个数（索引为1和2，因为Python从0开始计数）
    engine_switch = int(arr[1])  # 第二个数
    engine_result = is_status_200(arr[2])  # 第三个数
    if engine_switch == 1:
        arr2[0] = float(arr2[0]) + 1
    if engine_result:
        arr2[1] = float(arr2[1]) + 1
    return [arr2[0], arr2[1]]


# 处理日志文件块并更新统计信息
def get_arr_qps(arr):
    try:
        request_count = int(arr[1])  # 第二个数
        return request_count
    except ValueError as e:
        log_message("Error parsing arr fields: %s" % str(e))
        return 0


def is_status_200(engine_result):
    if len(engine_result) < 3:
        log_message("engine_result does not contain enough elements.")
        return False

    try:
        # 解析 JSON 字符串
        # json_data = json.loads(engine_result[2])
        json_data = engine_result
        # 检查 status 是否为 200
        return json_data.get('status') == 200
    except ValueError as e:
        log_message("Error decoding JSON: {e}")
        return False
    except Exception as e:
        log_message("Unexpected error: {e}")
        return False


def arr_success(arr, arr2):
    """
    计算数组中第三个数除以第二个数的百分比
    
    :param arr2: 按秒缓存请求数与成功数
    :param arr: 输入数组（列表）
    :return: 百分比（浮点数），如果出错则返回 None
    """
    # 确保数组至少有3个元素
    if len(arr) < 3:
        # raise ValueError("数组长度不足，至少需要3个元素")
        return [arr2[0], arr2[1]]
    # 获取第二个和第三个数（索引为1和2，因为Python从0开始计数）
    second_num = float(arr[1])  # 第二个数
    third_num = float(arr[2])  # 第三个数
    arr2[0] = float(arr2[0]) + second_num
    arr2[1] = float(arr2[1]) + third_num
    return [arr2[0], arr2[1]]


def percentage_calculation(arr):
    second_num = float(arr[0])  # 第二个数
    third_num = float(arr[1])  # 第三个数
    if second_num == 0:
        return 100
    # 计算百分比（第三个数 / 第二个数 * 100）
    percentage = (third_num / second_num) * 100
    return percentage


# 处理文件
def process_log_file(filename, read_pos):
    file_size = -1
    try:
        if not os.path.isfile(filename):
            return read_pos, file_size

        file_size = os.path.getsize(filename)
        if read_pos >= file_size:
            read_pos = file_size  # Adjust read position if it exceeds the file size
            return read_pos, file_size

        with open(filename, 'rb') as f:
            f.seek(read_pos)
            while True:
                remaining_bytes = file_size - f.tell()  # 计算剩余字节数
                if remaining_bytes == 0:
                    break

                # 每次读取剩余的字节数或 READ_CHUNK 大小，以较小的那个为准
                chunk_size = min(remaining_bytes, READ_CHUNK)
                chunk = f.read(chunk_size)
                if not chunk:
                    break

                # 处理日志文件块
                read_pos = process_log_chunk(chunk, read_pos)
    except Exception as e:
        log_message("Error processing old file: %s" % str(e))

    return read_pos, file_size


# 输出引擎统计结果到控制台和文件
def output_engine(save_reqs, local_ip):
    sorted_items = sorted(save_reqs.items(), key=lambda item: item[0])
    metric = 'edge.eng_time'
    for timestamp, count in sorted_items:
        print("%s %d %d ip=%s" % (metric, timestamp, count, local_ip))
        log_message("%s %d %d ip=%s" % (metric, timestamp, count, local_ip))


# 主逻辑函数
def convert_arr_as_percent(global_arr):
    for timestamp, success_counts in global_arr.items():
        if isinstance(success_counts, list) and len(success_counts) == 2:
            percentage = percentage_calculation(success_counts)
            global_arr[timestamp] = percentage
        else:
            global_arr[timestamp] = 100
            log_message("Unexpected format for success counts at timestamp {timestamp}: {success_counts}")
    return global_arr


def main():
    # 配置 logger 和文件处理器
    setup_logger("/tmp/nginx_process_debug_logger.log")
    # 关闭 logger
    # close_logger()

    local_ip = get_local_ip()
    while True:
        start_time = time.time()
        current_time = datetime.datetime.now()

        # 生成当前应处理的日志文件名
        log_file, last_hour_file_name, last_two_hour_file_name = generate_log_filename(LOG_DIR, start_time)

        # 读取 pos 文件
        last_filename, last_read_pos, now_filename, now_read_pos = read_pos_file(FILE_POS_FILE)
        # 大于 1 个小时启动的情况，不处理日志，上一小时合理范围为 上一小时和上二小时
        if last_filename != last_hour_file_name and last_filename != last_two_hour_file_name:
            write_pos_file(FILE_POS_FILE, last_hour_file_name, -1, log_file, 0)
            continue

        log_rotated = False
        # 日志文件已经切换，
        if last_filename == last_two_hour_file_name:
            log_rotated = True
        if now_filename == last_hour_file_name:
            log_rotated = True

        # 检查日志文件名是否发生变化
        if log_rotated:
            last_file_size = -1
            # 如果日志切换，先处理完旧文件
            # [now_filename == last_hour_file_name , now_read_pos == last_hour_file_name read pos]
            last_read_pos, last_file_size = process_log_file(last_hour_file_name, now_read_pos)
            output_stat(start_time, local_ip)
            # 更新 pos 文件，写入新文件的信息
            write_pos_file(FILE_POS_FILE, last_hour_file_name, last_file_size, log_file, 0)

            if (time.time() - start_time) >= 1:
                continue
            # 延迟 1s
            time.sleep(1)
            continue
        else:
            # 继续处理当前文件
            now_read_pos, file_size = process_log_file(log_file, now_read_pos)
            output_stat(start_time, local_ip)
            # 更新 pos 文件，写入新文件的信息
            log_message("log_file=%s,file_size=%d, now_read_pos=%d" % (log_file, file_size, now_read_pos))
            write_pos_file(FILE_POS_FILE, last_filename, last_read_pos, log_file, now_read_pos)

        # 输出 req 统计信息
        save_reqs = process_global_timedict(GLOBAL_REQS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        output_reqs(save_reqs, local_ip)

        # 输出 超时 统计信息
        save_timediffs = process_global_timedict(GLOBAL_TIMEDIFFS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        output_timediffs(save_timediffs, local_ip)

        # 输出 超时 统计信息
        # get_time_avg(GLOBAL_TIMEDIFFS_AVG)
        save_timediffs_avg = process_global_timedict(GLOBAL_TIMEDIFFS_AVG, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        get_time_avg(save_timediffs_avg)
        output_reqs_by_name(save_timediffs_avg, local_ip, "edge.timediff_avg")

        # 输出 引擎耗时
        # get_time_avg(GLOBAL_ENGINE_TIME)
        save_eng_time = process_global_timedict(GLOBAL_ENGINE_TIME, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        get_time_avg(save_eng_time)
        output_reqs_by_name(save_eng_time, local_ip, "edge.engine_time")

        # 输出 引擎成功率
        # convert_arr_as_percent(GLOBAL_ENGINE_SUCCESS)
        save_engine_success = process_global_timedict(GLOBAL_ENGINE_SUCCESS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        convert_arr_as_percent(save_engine_success)
        output_reqs_by_name(save_engine_success, local_ip, "edge.engine_success")

        # 输出 引擎QPS
        save_engine_qps = process_global_timedict(GLOBAL_ENGINE_QPS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        output_reqs_by_name(save_engine_qps, local_ip, "edge.engine_qps")

        # 输出 风控redis耗时
        # get_time_avg(GLOBAL_REDIS_TIME)
        save_redis_time = process_global_timedict(GLOBAL_REDIS_TIME, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        get_time_avg(save_redis_time)
        output_reqs_by_name(save_redis_time, local_ip, "edge.redis_time")

        # 输出 风控redis成功率
        # convert_arr_as_percent(GLOBAL_REDIS_SUCCESS)
        save_redis_success = process_global_timedict(GLOBAL_REDIS_SUCCESS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        convert_arr_as_percent(save_redis_success)
        output_reqs_by_name(save_redis_success, local_ip, "edge.redis_success")

        # 输出 风控redisQPS
        save_redis_qps = process_global_timedict(GLOBAL_REDIS_QPS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        output_reqs_by_name(save_redis_qps, local_ip, "edge.redis_qps")

        # 输出 usermapper耗时
        # get_time_avg(GLOBAL_USERMAPPER_TIME)
        save_usermapping_time = process_global_timedict(GLOBAL_USERMAPPER_TIME, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        get_time_avg(save_usermapping_time)
        output_reqs_by_name(save_usermapping_time, local_ip, "edge.usermapping_time")

        # 输出 usermapper成功率
        # convert_arr_as_percent(GLOBAL_USERMAPPER_SUCCESS)
        save_usermapping_success = process_global_timedict(GLOBAL_USERMAPPER_SUCCESS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        convert_arr_as_percent(save_usermapping_success)
        output_reqs_by_name(save_usermapping_success, local_ip, "edge.usermapping_success")

        # 输出 usermapperQPS
        save_usermapper_qps = process_global_timedict(GLOBAL_USERMAPPER_QPS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        output_reqs_by_name(save_usermapper_qps, local_ip, "edge.usermapping_qps")

        # 输出 realtime耗时
        # get_time_avg(GLOBAL_REALTIME_TIME)
        save_realtime_time = process_global_timedict(GLOBAL_REALTIME_TIME, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        get_time_avg(save_realtime_time)
        output_reqs_by_name(save_realtime_time, local_ip, "edge.realtime_time")

        # 输出 realtime成功率
        # convert_arr_as_percent(GLOBAL_REALTIME_SUCCESS)
        save_realtime_success = process_global_timedict(GLOBAL_REALTIME_SUCCESS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        convert_arr_as_percent(save_realtime_success)
        output_reqs_by_name(save_realtime_success, local_ip, "edge.realtime_success")

        # 输出 realtimeQPS
        save_realtime_qps = process_global_timedict(GLOBAL_REALTIME_QPS, SAVE_MINUTE_BEG, SAVE_MINUTE_END)
        output_reqs_by_name(save_realtime_qps, local_ip, "edge.realtime_qps")

        # sleep internal
        sys.stdout.flush()

        current_time = datetime.datetime.now()
        current_seconds = current_time.second
        time.sleep(60 - current_seconds + 1)


# Define time buckets (in milliseconds)
TIME_BUCKETS = {
    '0-200': (0, 200),
    '200-500': (200, 500),
    '500-1000': (500, 1000),
    '1000-1500': (1000, 1500),
    '1500-inf': (1500, float('inf'))
}

if __name__ == '__main__':
    main()
