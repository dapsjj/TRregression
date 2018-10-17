# -*- coding: UTF-8 -*-
import MeCab
import re
import collections
import pymssql
import datetime
import time
import logging
import os
import configparser
import decimal #不加打包成exe会出错


no_need_words = ["ccc","CCC"]



def get_year_week_from_Mst_date(current_date):
    '''
    :param current_date:系统当前日期年-月-日
    :return:Mst_date表返回的当前年和当前周
    '''
    try:
        # conn = pymssql.connect(server, user, password, database)
        # cur = conn.cursor()
        sql = " select year_no,week_no from Mst_date where date_mst='%s' "  % current_date
        cur.execute(sql)
        rows = cur.fetchall()
        if rows != []:
            current_year = rows[0][0]
            current_week = rows[0][1]
            return current_year,current_week
        else:
            return ""
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_year_week_from_Mst_date() error!Can not query from table Mst_date!")
        logger.error("Exception:" + str(ex))
        raise ex
    # finally:
    #     conn.close()


def read_dateConfig_file_set_database():
    if os.path.exists(os.path.join(os.path.dirname(__file__), "dateConfig.ini")):
        try:
            conf = configparser.ConfigParser()
            conf.read(os.path.join(os.path.dirname(__file__), "dateConfig.ini"), encoding="utf-8-sig")
            server = conf.get("server", "server")
            user = conf.get("user", "user")
            password = conf.get("password", "password")
            database = conf.get("database", "database")

            return server,user,password,database
        except Exception as ex:
            logger.error("Content in dateConfig.ini about database has error.")
            logger.error("Exception:" + str(ex))
            raise ex


def read_dateConfig_file_set_year_week():
    global affiliation
    global start_year
    global start_week
    global end_year
    global end_week
    global generate_year
    global generate_week
    global continue_weeks

    if os.path.exists(os.path.join(os.path.dirname(__file__), "dateConfig.ini")):
        try:
            conf = configparser.ConfigParser()
            conf.read(os.path.join(os.path.dirname(__file__), "dateConfig.ini"), encoding="utf-8-sig")
            affiliation = conf.get("affiliation", "affiliation")
            start_year = conf.get("start_year", "start_year")
            start_week = conf.get("start_week", "start_week")
            end_year = conf.get("end_year", "end_year")
            end_week = conf.get("end_week", "end_week")
            generate_year = conf.get("generate_year", "generate_year")
            generate_week = conf.get("generate_week", "generate_week")
            continue_weeks = conf.get("continue_weeks", "continue_weeks")

        except Exception as ex:
            logger.error("Content in dateConfig.ini  has error.")
            logger.error("Exception:" + str(ex))
            raise ex


def write_log():
    '''
    :return: 返回logger对象
    '''
    # 获取logger实例，如果参数为空则返回root logger
    logger = logging.getLogger()
    now_date = datetime.datetime.now().strftime('%Y%m%d')
    log_file = now_date+".log"# 文件日志
    if not os.path.exists("log"):#python文件同级别创建log文件夹
        os.makedirs("log")
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)s line:%(lineno)s %(message)s')
    file_handler = logging.FileHandler("log" + os.sep + log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter) # 可以通过setFormatter指定输出格式
    # 为logger添加的日志处理器，可以自定义日志处理器让其输出到其他地方
    logger.addHandler(file_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    logger.setLevel(logging.INFO)
    return logger

def getConn():
    global conn
    global cur
    try:
        conn = pymssql.connect(server, user, password, database)
        cur = conn.cursor()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method getConn() error!")
        raise ex

def get_data_from_report_keyword_property():
    global start_year
    global start_week
    global end_year
    global end_week
    global current_year
    global current_week

    if start_year and start_week and end_year and end_year and end_week:
        if int(start_week)<10:
            start_week="0" + start_week
        if int(end_week)<10:
            end_week="0" + end_week
    elif continue_weeks:
        year_week = "%s-W%s" % (current_year, current_week)
        year_month_day = datetime.datetime.strptime(year_week + '-0', "%Y-W%W-%w")
        year_ago_start=(year_month_day - datetime.timedelta(weeks=int(continue_weeks))).strftime('%Y')
        week_ago_start=(year_month_day - datetime.timedelta(weeks=int(continue_weeks))).strftime('%V')
        year_ago_end = (year_month_day - datetime.timedelta(weeks=int(1))).strftime('%Y')
        week_ago_end = (year_month_day - datetime.timedelta(weeks=int(1))).strftime('%V')
        start_year=year_ago_start
        start_week=week_ago_start
        end_year=year_ago_end
        end_week=week_ago_end







if __name__=="__main__":
    logger = write_log()  # 获取日志对象
    time_start = datetime.datetime.now()
    start = time.clock()
    logger.info("Program start,now time is:"+str(time_start))
    server,user,password,database = read_dateConfig_file_set_database()#读取配置文件中的数据库信息
    conn = None#连接
    cur = None#游标
    affiliation = None#部署
    start_year = None#开始年
    start_week = None#开始周
    end_year = None#结束年
    end_week = None#结束周
    generate_year = None#生成年
    generate_week = None#生成周
    continue_weeks = None#持续几周
    getConn()#数据库连接对象
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")#系统当前日期
    current_year,current_week = get_year_week_from_Mst_date(current_date)#从Mst_date获取当前年和周
    read_dateConfig_file_set_year_week()  # 读配置文件设置report_year和report_week
    get_data_from_report_keyword_property()
    # report_year = str(current_year)#当前系统年
    # report_week = str(current_week) #当前系统周
    # logger.info("report_year:" + report_year)
    # logger.info("report_week:" + report_week)
    time_end = datetime.datetime.now()
    end = time.clock()
    logger.info("Program end,now time is:"+str(time_end))
    logger.info("Program run : %f seconds" % (end - start))

