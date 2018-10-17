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

no_need_words1 = ["？","?"]
no_need_words2 = ["っ","ぁ","ぃ","ぅ","ぇ","ヶ"]
keep_words1 = ["ccc","CCC"]


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

def closeConn():
    global conn
    global cur
    try:
        cur.close()
        conn.close()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method closeConn() error!")
        raise ex

def get_report_keyword_property_list():
    global start_year
    global start_week
    global end_year
    global end_week
    global current_year
    global current_week

    if start_year and start_week and end_year and end_week:
        if int(start_week)<10:
            start_week="0" + start_week
        if int(end_week)<10:
            end_week="0" + end_week
        list1_report_keyword_property = get_data_from_report_keyword_property(start_year,start_week,end_year,end_week,affiliation,continue_weeks)
        return list1_report_keyword_property
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
        list1_report_keyword_property = get_data_from_report_keyword_property(start_year, start_week, end_year, end_week, affiliation, continue_weeks)
        return list1_report_keyword_property

def get_data_from_report_keyword_property(para_start_year,para_start_week,para_end_year,para_end_week,para_affiliation,para_continue_weeks):
    global str_start_year_week
    global str_end_year_week
    try:
        str_start_year_week=para_start_year + para_start_week
        str_end_year_week=para_end_year + para_end_week
        if para_affiliation:
            sql = "select distinct keyword as '辞書'," \
                  "min(free1) as '詞性'," \
                  "sum(keyword_frequency) as '頻度合計'," \
                  "avg(importance_degree) as '重要度'" \
                  "from [TRIAL].[dbo].[report_keyword_property] t1 " \
                  "inner join [dbEmployee].[dbo].[mstEmployeeBasic] t2 " \
                  "on t1.employee_code = t2.EmployeeCode " \
                  "inner join [dbEmployee].[dbo].[mstAttribute] t3 " \
                  "on t2.EmployeeManagementID=t3.EmployeeManagementID " \
                  "where cast(report_year as VARCHAR) + right('00' + cast(report_week as VARCHAR), 2) between %s and %s " \
                  "and t3.Affiliation=%s " \
                  "group by keyword " \
                  "order by '重要度' desc,'頻度合計' desc" \
                  % (str_start_year_week, str_end_year_week,para_affiliation)
        else:
            sql = "select distinct keyword as '辞書'," \
                  "min(free1) as '詞性'," \
                  "sum(keyword_frequency) as '頻度合計'," \
                  "avg(importance_degree) as '重要度'" \
                  "from [TRIAL].[dbo].[report_keyword_property] " \
                  "where cast(report_year as VARCHAR) + right('00' + cast(report_week as VARCHAR), 2) between %s and %s " \
                  "group by keyword " \
                  "order by '重要度' desc,'頻度合計' desc" \
                  % (str_start_year_week, str_end_year_week)
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            # for row in rows:
            #     report_keyword_property_list.append(list(row))
            list2_report_keyword_property_list = [list(row) for row in rows]
            return list2_report_keyword_property_list
        else:
            return ""
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method get_data_from_report_keyword_property() error!")
        logger.error("Exception:" + str(ex))
        raise ex


def no_need_keyword_remove():
    keepWordList1=[]
    keepWordList2=[]
    keepWordList3=[]
    for item1 in report_keyword_property_list:
        flag1 = False
        for mark1 in no_need_words1: #["？","?"]
            if item1[0].find(mark1)!=-1:
                flag1 = True
                break
        if flag1==False:
            keepWordList1.append(item1)

    for item2 in keepWordList1:
        flag2 = False
        for mark2 in no_need_words2: #["っ","ぁ","ぃ","ぅ","ぇ","ヶ"]
            if item2[0].startswith(mark2) or item2[0].endswith(mark2):
                flag2 = True
        if flag2 == False:
            keepWordList2.append(item2)

    for item3 in keepWordList2:
        if item3[0] in keep_words1: # ["ccc","CCC"]
            keepWordList3.append(item3)
        else:
            str_repeat_list=[]
            if len(str_repeat_list)<3: #长度小于3的词加入到List
                keepWordList3.append(item3)
            elif len(str_repeat_list)>=3: #长度大于3的词
                str_repeat_list = [everyChar for everyChar in item3[0]] #把词中的每一个字放到str_repeat_list
                str_repeat_list = list(set(str_repeat_list)) #利用set特性去重
                if len(str_repeat_list)>1: #不全是同一个字符则加入到List中
                    keepWordList3.append(item3)


    return keepWordList3








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
    str_start_year_week = None
    str_end_year_week = None
    getConn()#数据库连接对象
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")#系统当前日期
    current_year,current_week = get_year_week_from_Mst_date(current_date)#从Mst_date获取当前年和周
    read_dateConfig_file_set_year_week()  # 读配置文件设置report_year和report_week
    report_keyword_property_list = get_report_keyword_property_list()
    report_keyword_property_list = no_need_keyword_remove()
    logger.info("start year week:" + str_start_year_week)
    logger.info("end year week:" + str_end_year_week)
    logger.info("affiliationk:" + affiliation)
    logger.info("continue_weeks:" + continue_weeks)
    closeConn()
    time_end = datetime.datetime.now()
    end = time.clock()
    logger.info("Program end,now time is:"+str(time_end))
    logger.info("Program run : %f seconds" % (end - start))

