import datetime

import pymysql
import requests
import threading
from threading import Lock
import queue
import Constant

# 全局锁
g_lock = Lock()

def fetch_web_data(url,timeout=10):
    try:
        r = requests.get(url,timeout=timeout)
        response_time = r.elapsed.total_seconds()
        return str(response_time)
    except Exception as e:
        return None


class FetchListThread(threading.Thread):

    def __init__(self, mq):
        threading.Thread.__init__(self)
        self.__mq = mq

    def run(self):
        '''
        获取各省市的url，保存到mq
        :return:
        '''
        [self.__mq.put(Constant.ALL_LOGIN_URL[tax_name]) for tax_name in Constant.ALL_TAX_NAME]


class IPCheckThread(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.__queue = queue

    def run(self):
        while True:
            try:
                login_url = self.__queue.get(timeout=10)
            except Exception as e:
                return False
            data = fetch_web_data(login_url, timeout=10)
            print(f'当前url==》{login_url}响应时长为==》{data}')
            g_lock.acquire()
            self.sql_insert(login_url, data)
            g_lock.release()

    def sql_insert(self, request_url, response_time):
        '''
        批量插入数据库
        :return:
        '''
        for tax_nam, login_url in Constant.ALL_LOGIN_URL.items():
            if login_url == request_url:
                tax_area_code = Constant.ALL_TAX_AREA_CODE[tax_nam]
                update_time = datetime.datetime.now()
                conn = pymysql.connect(host=Constant.DB_CONFIG['host'],
                                       port=Constant.DB_CONFIG['port'],
                                       user=Constant.DB_CONFIG['user'],
                                       passwd=Constant.DB_CONFIG['passwd'],
                                       charset=Constant.DB_CONFIG['charset'],
                                       db=Constant.DB_CONFIG['db'])
                cursor = conn.cursor()
                try:
                    sql_select = "SELECT tax_name From tax_web where tax_name = %s"
                    cursor.execute(sql_select, tax_nam)
                    results = cursor.fetchall()
                    conn.commit()
                    # 如果查询到该条数据，执行修改操作，否则执行添加操作
                    if results:
                        sql = "update tax_web set response_time = %s, update_time = %s where tax_name = %s"
                        # 开启事物
                        conn.begin()
                        cursor.execute(sql, (response_time, update_time, tax_nam))
                    else:
                        sql = "INSERT INTO tax_web (tax_name, tax_area_code, request_url, response_time, update_time, is_delete) VALUES (%s,%s,%s,%s,%s,%s)"
                        conn.begin()
                        cursor.execute(sql, (tax_nam, tax_area_code, request_url, response_time, update_time, 0))
                    # 提交
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(str(e) + tax_nam + ":" + request_url)
                    # 报错事务回滚
                finally:
                    # 关闭连接
                    cursor.close()
                    conn.close()


def process():
    # 定时器构造函数主要有2个参数，第一个参数为时间，第二个参数为函数名
    timer = threading.Timer(60*60*60, process)  # 1h调用一次函数
    timer.start()
    mq = queue.Queue()
    fth = FetchListThread(mq)
    thread_num = 10
    thread_list = []
    for i in range(thread_num):
        t = IPCheckThread(mq)
        thread_list.append(t)

    fth.start()
    [th.start() for th in thread_list]

    fth.join()
    [th.join() for th in thread_list]

    print('all work has done')

if __name__ == '__main__':
    process()

'''
sql语句：
CREATE TABLE tax_web(
 id int(10) PRIMARY KEY AUTO_INCREMENT, 
 tax_name  VARCHAR(64), 
 tax_area_code VARCHAR(32),
 request_url  VARCHAR(255), 
 response_time  VARCHAR(32) DEFAULT NULL, 
 create_time datetime DEFAULT NOW(),
 update_time datetime DEFAULT NULL,
 is_delete  int(1) DEFAULT 0
)
'''