# -*- coding: UTF-8 -*-
import MeCab
import re
import collections
import numpy as np
import pandas as pd
import pymssql
import datetime
import time
from sklearn.linear_model import LinearRegression
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
    finally:
        cur.close()
        conn.close()

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


def calculate_average(paraList,index):
    # df = pd.DataFrame(report_keyword_property_list)
    df = pd.DataFrame(paraList)
    average_value = df.iloc[:, index].mean() #下标2是頻度合計
    return average_value


def calculate_standard_deviation(paraList,index):
    # df = pd.DataFrame(report_keyword_property_list)
    df = pd.DataFrame(paraList)
    colum_value = df.iloc[:, index]#下标2是頻度合計
    standard_deviation = np.std(colum_value, ddof=1)
    return standard_deviation


def set_generate_year_generate_week():
    global generate_year
    global generate_week
    if not generate_year or not generate_week:
        year_week = "%s-W%s" % (current_year, current_week)
        year_month_day = datetime.datetime.strptime(year_week + '-0', "%Y-W%W-%w")
        generate_year = (year_month_day - datetime.timedelta(weeks=int(1))).strftime('%Y')
        generate_week = (year_month_day - datetime.timedelta(weeks=int(1))).strftime('%V')


def calculate_frequency_deviation_value():
    deviation_constant1 = 50
    deviation_constant2 = 10
    for i in range(len(report_keyword_property_list)):
        keyword_frequency_deviation = deviation_constant1+(report_keyword_property_list[i][2]-keyword_frequency_avg)/keyword_frequency_offet*deviation_constant2
        report_keyword_property_list[i].append(keyword_frequency_deviation)
    return report_keyword_property_list


def delete_data_from_importance_frequency_deviation():
    try:
        sql = ' delete from 重要度頻度 where report_year = %s and report_week = %s' \
              % (generate_year, generate_week)
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method delete_data_from_importance_frequency_deviation() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex


def insert_into_importance_frequency_deviation():
    if report_keyword_property_list:
        try:
            insert_importance_frequency_deviation_list = [tuple([generate_year, generate_week, *item]) for item in report_keyword_property_list]
            sql = ' insert into 重要度頻度 (report_year, report_week, keyword, property, keyword_frequency	, importance_degree, keyword_frequency_offet) ' \
                  ' values(%s,%s,%s,%s,%s,%s,%s) '
            cur.executemany(sql, insert_importance_frequency_deviation_list)
            conn.commit()
            return insert_importance_frequency_deviation_list
        except pymssql.Error as ex:
            logger.error("dbException:" + str(ex))
            raise ex
        except Exception as ex:
            logger.error("Call method insert_into_importance_frequency() error!")
            logger.error("Exception:" + str(ex))
            conn.rollback()
            raise ex


def calculate_Intercept_X_Variable(para_list):
    df = pd.DataFrame(para_list)
    X = df.iloc[:, 5]
    y = df.iloc[:, 6]
    X = X.values.reshape(-1, 1)
    y = y.values.reshape(-1, 1)
    clf = LinearRegression()
    clf.fit(X, y)
    para_Intercept = clf.intercept_[0]
    para_X_Variable_1 = clf.coef_[0][0]
    return para_Intercept,para_X_Variable_1


def delete_data_from_importance_classification():
    try:
        sql = ' delete from 重要度分類 where report_year = %s and report_week = %s' \
              % (generate_year, generate_week)
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method delete_data_from_importance_classification() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex


def generate_year_week_keyword_property_list():
    property_list = []
    temp_importance_classification_list = report_keyword_property_list.copy()
    for item in temp_importance_classification_list:
        property_list.append([generate_year,generate_week,item[0],item[1]])
    return property_list




def calculate_importance_classification_value(property_list):
    importance_classification_list = property_list.copy()
    if len(importance_classification_list) == len(report_keyword_property_list):
        for i in range(len(report_keyword_property_list)):
            importance_degree_g = float(report_keyword_property_list[i][3])*X_Variable_1 + Coefficients_Intercept
            importance_classification_list[i].append(importance_degree_g)
        return importance_classification_list




def insert_into_importance_classification(importance_classification_list):
    if importance_classification_list:
        importance_classification_list = [tuple(item) for item in importance_classification_list ]
        try:
            sql = ' insert into 重要度分類 (report_year, report_week, keyword, property, importance_degree_g) ' \
                  ' values(%s,%s,%s,%s,%s) '
            cur.executemany(sql, importance_classification_list)
            conn.commit()
        except pymssql.Error as ex:
            logger.error("dbException:" + str(ex))
            raise ex
        except Exception as ex:
            logger.error("Call method insert_into_importance_classification() error!")
            logger.error("Exception:" + str(ex))
            conn.rollback()
            raise ex


def calculate_adjustment(importance_classification_list):
    if importance_classification_list:
        df = pd.DataFrame(importance_classification_list)
        X = df.iloc[:, 4]
        max_value=max(X)
        calculate_result = (max_value - importance_degree_g_avg)*importance_degree_g_offet
        return calculate_result


def calculate_pn_value(importance_classification_list):
    keyword_pn_list = importance_classification_list.copy()
    if len(keyword_pn_list) == len(report_keyword_property_list):
        for i in range(len(keyword_pn_list)):
            pn = (keyword_pn_list[i][4] - importance_degree_g_avg)/adjustment*importance_degree_g_offet
            keyword_pn_list[i].append(pn)
        for item in keyword_pn_list:
            del item[4]
        return keyword_pn_list


def delete_data_from_pn_dictionary():
    try:
        sql = ' delete from ネガポジ辞書 where report_year = %s and report_week = %s' \
              % (generate_year, generate_week)
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method delete_data_from_pn_dictionary() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex

def insert_into_pn_dictionary(pn_list):
    if pn_list:
        keyword_pn_list = [tuple(item) for item in pn_list ]
        try:
            sql = ' insert into ネガポジ辞書 (report_year, report_week, keyword, property, pn) ' \
                  ' values(%s,%s,%s,%s,%s) '
            cur.executemany(sql, keyword_pn_list)
            conn.commit()
        except pymssql.Error as ex:
            logger.error("dbException:" + str(ex))
            raise ex
        except Exception as ex:
            logger.error("Call method insert_into_pn_dictionary() error!")
            logger.error("Exception:" + str(ex))
            conn.rollback()
            raise ex


def calculate_negative_positive_value(set_year,set_week):
    if set_year and set_week:
        try:
            sql = " select t1.report_year as '年', " \
                  " t1.report_week as '週', " \
                  " t1.employee_code as '社員番号', " \
                  " SUM(CASE WHEN  t2.pn<0 THEN  t2.pn ELSE 0 END) AS 'ネガ合計', " \
                  " SUM(CASE WHEN t2.pn>0 THEN t2.pn ELSE 0 END) AS 'ポジ合計' " \
                  " from report_keyword_property t1 inner join ネガポジ辞書 t2 on t1.keyword=t2.keyword " \
                  " where t1.report_year =%s and t1.report_week =%s " \
                  " and t2.report_year =%s and t2.report_week =%s " \
                  " group by t1.report_year,t1.report_week,t1.employee_code " \
                  " order by t1.report_year,t1.report_week,t1.employee_code " \
                  % (set_year, set_week, set_year, set_week)
            cur.execute(sql)
            rows = cur.fetchall()
            if rows:
                negative_positive_list = [list(row) for row in rows]
                return negative_positive_list
            else:
                return ""
        except pymssql.Error as ex:
            logger.error("dbException:" + str(ex))
            raise ex
        except Exception as ex:
            logger.error("Call method calculate_negative_positive_value() error!")
            logger.error("Exception:" + str(ex))
            raise ex

def delete_data_from_employee_negative_positive():
    try:
        sql = ' delete from ネガポジ_個人別 where report_year = %s and report_week = %s' \
              % (generate_year, generate_week)
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method delete_data_from_employee_negative_positive() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex


def insert_into_employee_negative_positive(employee_negative_positive_list):
    if employee_negative_positive_list:
        keyword_pn_list = [tuple(item) for item in employee_negative_positive_list ]
        try:
            sql = ' insert into ネガポジ_個人別 (report_year, report_week, employeecode, negative, positive) ' \
                  ' values(%s,%s,%s,%s,%s) '
            cur.executemany(sql, keyword_pn_list)
            conn.commit()
        except pymssql.Error as ex:
            logger.error("dbException:" + str(ex))
            raise ex
        except Exception as ex:
            logger.error("Call method insert_into_employee_negative_positive() error!")
            logger.error("Exception:" + str(ex))
            conn.rollback()
            raise ex


def delete_data_from_parameter():
    try:
        sql = ' delete from パラメータ where report_year = %s and report_week = %s' \
              % (generate_year, generate_week)
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method delete_data_from_parameter() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex

def insert_into_parameter(set_year,set_week,para_keyword_frequency_avg,para_keyword_frequency_offet,para_importance_degree_g_avg,para_importance_degree_g_offet,para_adjustment,para_Coefficients_Intercept,para_X_Variable_1):
    try:
        sql = ' insert into パラメータ (report_year, report_week, keyword_frequency_avg, keyword_frequency_offet, importance_degree_g_avg, importance_degree_g_offet, adjustment, Coefficients_Intercept, Coefficients_X_Variable_1) ' \
              ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s) ' \
              % (set_year,set_week,para_keyword_frequency_avg,para_keyword_frequency_offet,para_importance_degree_g_avg,para_importance_degree_g_offet,para_adjustment,para_Coefficients_Intercept,para_X_Variable_1)
        cur.execute(sql)
        conn.commit()
    except pymssql.Error as ex:
        logger.error("dbException:" + str(ex))
        raise ex
    except Exception as ex:
        logger.error("Call method insert_into_parameter() error!")
        logger.error("Exception:" + str(ex))
        conn.rollback()
        raise ex



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
    current_date = datetime.datetime.now().strftime("%Y-%m-%d") #系统当前日期
    current_year,current_week = get_year_week_from_Mst_date(current_date) #从Mst_date获取当前年和周
    read_dateConfig_file_set_year_week()  # 读配置文件设置参数
    set_generate_year_generate_week() #设置generate_year和generate_week,如果generate_year为空或者generate_week为空,则取当前日期对应的年和周前一周所在的年和周
    report_keyword_property_list = get_report_keyword_property_list() #从数据库取出指定年周的数据
    report_keyword_property_list = no_need_keyword_remove() #去掉没有用的关键字
    keyword_frequency_avg = calculate_average(report_keyword_property_list,2) #用"頻度合計"计算"頻度平均"
    keyword_frequency_offet = calculate_standard_deviation(report_keyword_property_list,2) #用"頻度合計"计算"頻度標準偏差"
    report_keyword_property_list = calculate_frequency_deviation_value() #頻度偏差値=50+(某一列的頻度合計-頻度平均)/頻度標準偏差*10,计算出这个值后加入到report_keyword_property_list中
    delete_data_from_importance_frequency_deviation() #插入到"重要度頻度"前先删除数据
    list_for_calculate_Coefficients_Intercept_X_Variable_1=insert_into_importance_frequency_deviation() #插入到表"重要度頻度",字段"提出年"、"週"、"id"、"キーワード"、"詞性"、"頻度"、"重要度"、"頻度偏差値"
    Coefficients_Intercept,X_Variable_1 = calculate_Intercept_X_Variable(list_for_calculate_Coefficients_Intercept_X_Variable_1) #用"頻度偏差値"和"重要度"做回帰分析,计算出"切片"(Coefficients_Intercept)和"X"(Coefficients_X_Variable_1)
    year_week_keyword_property_list = generate_year_week_keyword_property_list()#生成年、周、关键字、词性的List
    report_importance_classification_list = calculate_importance_classification_value(year_week_keyword_property_list) #重要度分类=重要度*X+切片,计算出这个值后加入到report_importance_classification_list中
    delete_data_from_importance_classification()  #插入到表"重要度分類"前删除数据
    insert_into_importance_classification(report_importance_classification_list)#插入到表"重要度分類",字段"提出年"、"週"、"キーワード"、"詞性"、"重要度分類"
    importance_degree_g_avg = calculate_average(report_importance_classification_list,4) #用"重要度分類"计算"重要度平均",通常是50
    importance_degree_g_offet = calculate_standard_deviation(report_importance_classification_list,4) #用"重要度分類"计算"重要度分類標準偏差"
    adjustment = calculate_adjustment(report_importance_classification_list) #计算調整引数(常量),調整引数(常量)=(重要度分類最大值-重要度平均)*重要度分類標準偏差
    report_keyword_pn_list = calculate_pn_value(report_importance_classification_list) #ネガポジ值=(重要度分類 - 重要度平均)/计算調整引数(常量)*重要度分類標準偏差,计算出这个值后加入到report_keyword_pn_list中
    delete_data_from_pn_dictionary()  #插入到表"ネガポジ辞書"前删除数据
    insert_into_pn_dictionary(report_keyword_pn_list) #插入到表"ネガポジ辞書",字段"提出年"、"週"、"キーワード"、"詞性"、ネガポジ値"
    year_week_employee_negative_positive_list = calculate_negative_positive_value(generate_year,generate_week) #用生成的字典计算ネガポジ_個人別
    delete_data_from_employee_negative_positive() #插入到表"ネガポジ_個人別"前删除数据
    insert_into_employee_negative_positive(year_week_employee_negative_positive_list) #插入到表"重要度分類",字段"提出年"、"週"、"社員番号"、"キーワード"、"ネガ値"、"ポジ値"
    delete_data_from_parameter() #插入到表"パラメータ"前删除数据
    insert_into_parameter(generate_year,generate_week,keyword_frequency_avg,keyword_frequency_offet,importance_degree_g_avg,importance_degree_g_offet,adjustment,Coefficients_Intercept,X_Variable_1) #插入到表"パラメータ",字段"提出年"、"週"、"頻度平均"、"頻度標準偏差"、"重要度分類平均"、"重要度分類標準偏差"、"調整引数"、"Coefficients_Intercept"、"Coefficients_X_Variable_1"
    logger.info("start year week:" + str_start_year_week)
    logger.info("end year week:" + str_end_year_week)
    logger.info("affiliationk:" + affiliation)
    logger.info("continue_weeks:" + continue_weeks)
    logger.info("generate_year:" + generate_year)
    logger.info("generate_week:" + generate_week)
    print(keyword_frequency_avg,keyword_frequency_offet,importance_degree_g_avg,importance_degree_g_offet,Coefficients_Intercept,X_Variable_1)
    closeConn()
    time_end = datetime.datetime.now()
    end = time.clock()
    logger.info("Program end,now time is:"+str(time_end))
    logger.info("Program run : %f seconds" % (end - start))

