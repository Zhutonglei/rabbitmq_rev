# -*- coding: utf-8 -*-

import pika
import time
import re
import pymysql as mysqldb
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
class rabbitmq():
    def __init__(self,username,password,host,port):
        self.username = username
        self.password = password
        self.host = host
        self.port = port


    def __receive_message__(self,exchange,exchange_type,cursor,queuename,auto_ack,exclusive=False,durable=True,binding_keys=[]):
        credentials = pika.PlainCredentials(username=self.username, password=self.password)
        connection = pika.BlockingConnection( pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials))
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type,durable=durable)
        if exclusive==True:
            #exclusive 队列非持久化
            result = channel.queue_declare('', exclusive=exclusive)
        else:
            result = channel.queue_declare(queuename, exclusive=exclusive)

        queue_name = result.method.queue
        
        if not binding_keys:
            sys.stderr.write("{A}NOT IN  BINDING_KEYS".format(A=binding_key))
            sys.exit(1)

        for binding_key in binding_keys:
            channel.queue_bind(
                exchange=exchange, queue=queue_name, routing_key=binding_key)

        #print(' [*] Waiting for logs. To exit press CTRL+C')

        def callback(ch, method, properties, body):
            excute_sql="excute_sql"
            try:
                print(" [x] %r:%r" % (method.routing_key, body))

                # timestamp1 = cursor.execute("SELECT CURRENT_TIMESTAMP(3)")
                # timestamp1 = cursor.fetchone()
                # microsecond = float((timestamp1[0].microsecond))/1000000
                # timestamp1 =(str(timestamp1[0]).split("."))[0]
                #
                # timestamp1 = time.mktime(time.strptime(timestamp1, '%Y-%m-%d %H:%M:%S'))
                # timestamp1 =timestamp1+microsecond
                timestamp1=time.time()
                newbody = re.split("[:,=]",body)
                s_timestart = newbody[4]
                time_difference = timestamp1 - float(s_timestart)
                s_exchange = newbody[10]
                s_durable = newbody[16]
                s_binding_keys =newbody[14]
                s_count=newbody[2]
                s_type = newbody[12]
                # UPDATE rabbitmq_sender SET timeend="1",timedifference="2" WHERE exchange='exchange_420'and routing_key='routing_0'and count='1'
                # excute_sql = "INSERT INTO rabbitmq_counter VALUES('%(exchange)s','%(type)s','%(exclusive)s','%(durable)s','%(binding_keys)s','%(timestart)s','%(count)s','%(timeend)s','%(timedifference)s')" % {
                #     "exchange": s_exchange, "type":s_type,"exclusive": exclusive, "durable": s_durable, "binding_keys": s_binding_keys, "timestart":s_timestart ,
                #     "count": s_count, "timeend":timestamp1 , "timedifference": time_difference}
                excute_sql ="UPDATE rabbitmq_sender SET timeend= CURRENT_TIMESTAMP(3) WHERE exchange='{c}'and routing_key='{d}'and count='{e}'".format(c=s_exchange,d=s_binding_keys,e=s_count)
                print excute_sql
                cursor.execute(excute_sql)
                if auto_ack==False:
                    ch.basic_ack(delivery_tag = method.delivery_tag)
            except Exception as e:
                print str(e)
                logger = Logger(
                    logname='log_' + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d') + '.txt',
                    loglevel=1, logger='rabbitmq_received').getlog()
                logger.error('Error:{a},sql:{b}'.format(a=e,b=excute_sql))
        channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=auto_ack)

        channel.start_consuming()
    
    def __ReturnList__(self,grouping,origin_list=[]):
        new_list=[]
        start =0
        while start<len(origin_list):
            temp = []
            for  i  in range(start,start+grouping):
                try:
                     temp.append(origin_list[i])
                except:
                    pass
            new_list.append(temp)
            start =start+grouping
        return new_list

    def __MutiUserList__(self,grouping,origin_list=[]):
        new_list = []
        for k in range(0,len(origin_list)):
            for j in range(0,grouping):
                temp = []
                temp.append(origin_list[k])
                new_list.append(temp)
        return new_list



