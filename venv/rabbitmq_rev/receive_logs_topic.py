# coding=utf-8

from rabbitmq_receive import receive_log
import time
import threading
import configparser
import pymysql as mysqldb
import random
import  logging
import datetime

class Logger():
    def __init__(self, logname, loglevel, logger):
        '''
           指定保存日志的文件路径，日志级别，以及调用文件
           将日志存入到指定的文件中
        '''
        # 用字典保存日志级别
        format_dict =\
        {
            1: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            2: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            3: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            4: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            5: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        }
        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)


        # 创建一个handler，用于写入日志文件
        fh = logging.FileHandler(logname)
        fh.setLevel(logging.DEBUG)

        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # 定义handler的输出格式
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = format_dict[int(loglevel)]
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    def getlog(self):
        return self.logger

cf = configparser.ConfigParser()
cf.read("./config.ini")
exchange = cf.get("receive", "exchange1")
exchange1 = cf.get("receive", "exchange2")
exchange2 = cf.get("receive", "exchange3")
exchange3 = cf.get("receive", "exchange4")
exchange4 = cf.get("receive", "exchange5")
exchange5 = cf.get("receive", "exchange6")
routecount = cf.get("receive", "routecount")
routecount = int(routecount)
durable = cf.get("receive", "durable")
exclusive=cf.get("receive", "exclusive")
interval =cf.get("receive", "interval")
interval =float(interval)
threadnum = cf.get("receive", "threads")
threadnum = int(threadnum)
grouping = cf.get("receive","grouping")
grouping = int (grouping)
auto_ack=cf.get("receive","auto_ack")
if auto_ack=="True":
    auto_ack=True
else:
    auto_ack=False
host =cf.get("receive","host")

flag1= cf.get("receive", "flag1")
flag2= cf.get("receive", "flag2")
flag3= cf.get("receive", "flag3")
flag4= cf.get("receive", "flag4")
flag5= cf.get("receive", "flag5")
flag6= cf.get("receive", "flag6")
username =cf.get("receive","queuename")

if durable=='True':
    durable=True
else:
    durable=False

if exclusive =='True':
    exclusive=True
else:
    exclusive=False




receive_log = receive_log.rabbitmq(username='zntx',password='1qaz@WSX',host='192.168.21.136',port=5672)
routing_key_list =[]
for i in range(0,routecount):
    routing_key_list.append('routing_'+str(i))
routing_key_list=receive_log.__MutiUserList__(grouping,routing_key_list)
def MutiReceive(exchange,exchange_type,exclusive,durable,binding_keys,queuename):
    def on_message():
        # 写入数据库
        config = {
            'host': host,
            'port': 3306,
            'user': 'root',
            'passwd': '1qaz@WSX',
            'db': 'rabbitmq',
            'charset': 'utf8'
        }
        try:
            conn = mysqldb.connect(**config)
            cursor = conn.cursor()
            conn.autocommit(1)
            return cursor
        except Exception as e:
            logger = Logger(
                logname='log_' + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d') + '.txt',
                loglevel=1, logger='rabbitmq_received_mysql').getlog()
            logger.error('Error:{a}'.format(a=e))
            return None

    cursor = on_message()
    if cursor!=None:
        receive_log.__receive_message__(exchange=exchange, exchange_type=exchange_type, exclusive=exclusive,auto_ack=auto_ack, durable=durable,queuename=queuename,
                                        binding_keys=binding_keys,cursor=cursor)
        cursor.close()

threads=[]


if flag1=="True":
    for j in range(0, threadnum):
        binding_keys = routing_key_list[j]
        t = threading.Thread(target=MutiReceive, args=(exchange, 'topic', exclusive, durable, binding_keys,username+"queue_{a}_{b}".format(a=exchange,b=j)))
        threads.append(t)

if flag2=="True":
    for j in range(0, threadnum):
        binding_keys = routing_key_list[j]
        t = threading.Thread(target=MutiReceive, args=(exchange1, 'topic', exclusive, durable, binding_keys,username+"queue_{a}_{b}".format(a=exchange1,b=j)))
        threads.append(t)

if flag3=="True":
    for j in range(0, threadnum):
        binding_keys = routing_key_list[j]
        t = threading.Thread(target=MutiReceive, args=(exchange2, 'topic', exclusive, durable, binding_keys,username+"queue_{a}_{b}".format(a=exchange2,b=j)))
        threads.append(t)

if flag4=="True":
    for j in range(0, threadnum):
        binding_keys = routing_key_list[j]
        t = threading.Thread(target=MutiReceive, args=(exchange3, 'topic', exclusive, durable, binding_keys,username+"queue_{a}_{b}".format(a=exchange3,b=j)))
        threads.append(t)

if flag5=="True":
    for j in range(0, threadnum):
        binding_keys = routing_key_list[j]
        t = threading.Thread(target=MutiReceive, args=(exchange4, 'topic', exclusive, durable, binding_keys,username+"queue_{a}_{b}".format(a=exchange4,b=j)))
        threads.append(t)

if flag6=="True":
    for j in range(0, threadnum):
        binding_keys = routing_key_list[j]
        t = threading.Thread(target=MutiReceive, args=(exchange5, 'topic', exclusive, durable, binding_keys,username+"queue_{a}_{b}".format(a=exchange5,b=j)))
        threads.append(t)

for k in range(0,len(threads)):
    threads[k].start()
    
for m in range(0, len(threads)):
    threads[m].join()








