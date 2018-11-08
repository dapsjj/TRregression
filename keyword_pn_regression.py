# -*- coding: UTF-8 -*-

import numpy as np
import pandas as pd
import pymssql
import datetime
import time
from sklearn.linear_model import LinearRegression
from scipy.stats import linregress
import logging
import os
import configparser
import decimal #不加打包成exe会出错

no_need_words1 = ["？","?"]
no_need_words2 = ["っ","ぁ","ぃ","ぅ","ぇ","ヶ"]
keep_words1 = ["ccc","CCC"]

conn = None  # 连接
cur = None  # 游标
affiliated_company = None  # 部署
start_year = None  # 开始年
start_week = None  # 开始周
end_year = None  # 结束年
end_week = None  # 结束周
generate_year = None  # 生成年
generate_week = None  # 生成周
continue_weeks = None  # 持续几周
str_start_year_week = None
str_end_year_week = None


def get_year_week_from_Mst_date(current_date):
    '''
    :param current_date:系统当前日期年-月-日
    :return:Mst_date表返回的当前年和当前周
    '''
    try:
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


def read_dateConfig_file_set_database():
    '''
    读dateConfig.ini,设置数据库信息
    '''
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
    else:
        logger.error("DateConfig.ini doesn't exist!")


def read_dateConfig_file_set_year_week():
    '''
    读dateConfig.ini,获取部署、开始年、开始周、结束年、结束周、生成年、生成周、持续周
    '''
    global affiliated_company
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
            affiliated_company = conf.get("affiliated_company", "affiliated_company")
            start_year = conf.get("start_year", "start_year")
            start_week = conf.get("start_week", "start_week")
            end_year = conf.get("end_year", "end_year")
            end_week = conf.get("end_week", "end_week")
            generate_year = conf.get("generate_year", "generate_year")
            generate_week = conf.get("generate_week", "generate_week")
            continue_weeks = conf.get("continue_weeks", "continue_weeks")
        except Exception as ex:
            logger.error("Content in dateConfig.ini has error.")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("DateConfig.ini doesn't exist!")


def write_log():
    '''
    写log
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
    '''
    声明数据库连接对象
    '''
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
    '''
    关闭数据库连接对象
    '''
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
    '''
    从表report_keyword_property获取要处理的人员List,分2部分。
    第1种是开始年、开始周、结束年、结束周都有值的,则获取这个区间的List
    第2种是开始年、开始周、结束年、结束周有一个为空的,则获取系统当前时间向前的continue_weeks周作为开始时间,系统当前时间的前1周作为结束时间
    '''
    global start_year
    global start_week
    global end_year
    global end_week
    global current_year
    global current_week

    try:
        if start_year and start_week and end_year and end_week: #开始年、开始周、结束年、结束周都不为空时，走这个分支
            if int(start_week)<10:
                start_week="0" + start_week
            if int(end_week)<10:
                end_week="0" + end_week
            list1_report_keyword_property = get_data_from_report_keyword_property(start_year, start_week, end_year, end_week, affiliated_company)
            return list1_report_keyword_property
        elif continue_weeks: #开始年、开始周、结束年、结束周有1个为空时，走这个分支
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
            list1_report_keyword_property = get_data_from_report_keyword_property(start_year, start_week, end_year, end_week, affiliated_company)
            return list1_report_keyword_property
        else: #开始年、开始周、结束年、结束周有1个为空,持续周也为空,记录错误
            logger.error("Start_year、start_week、end_year、end_week、continue_weeks can't all null.")
            return
    except Exception as ex:
        logger.error("Call method get_report_keyword_property_list() error!")
        logger.error("Exception:" + str(ex))
        raise ex


def get_data_from_report_keyword_property(para_start_year,para_start_week,para_end_year,para_end_week,para_affiliated_company):
    '''
    从[TRIAL].[dbo].[report_keyword_property]获取数据
    :param para_start_year:开始年
    :param para_start_week:开始周
    :param para_end_year:结束年
    :param para_end_week:结束周
    :param para_affiliated_company:部署
    :return:表[TRIAL].[dbo].[report_keyword_property]的List
    '''
    global str_start_year_week
    global str_end_year_week
    try:
        str_start_year_week=para_start_year + para_start_week
        str_end_year_week=para_end_year + para_end_week
        if para_affiliated_company:
            sql = " select distinct keyword as '辞書'," \
                  " free1 as '詞性'," \
                  " sum(keyword_frequency) as '頻度合計'," \
                  " avg(importance_degree) as '重要度'" \
                  " from [TRIAL].[dbo].[report_keyword_property] t1 " \
                  " inner join [dbEmployee].[dbo].[mstEmployeeBasic] t2 " \
                  " on t1.employee_code = t2.EmployeeCode " \
                  " inner join [dbEmployee].[dbo].[mstAttribute] t3 " \
                  " on t2.EmployeeManagementID=t3.EmployeeManagementID " \
                  " where cast(report_year as VARCHAR) + right('00' + cast(report_week as VARCHAR), 2) between %s and %s " \
                  " and t3.affiliated_company=%s " \
                  " group by keyword,free1 " \
                  " order by '重要度' desc,'頻度合計' desc" \
                  % (str_start_year_week, str_end_year_week,para_affiliated_company)
        else:
            sql = " select distinct keyword as '辞書'," \
                  " free1 as '詞性'," \
                  " sum(keyword_frequency) as '頻度合計'," \
                  " avg(importance_degree) as '重要度'" \
                  " from [TRIAL].[dbo].[report_keyword_property] " \
                  " where cast(report_year as VARCHAR) + right('00' + cast(report_week as VARCHAR), 2) between %s and %s " \
                  " group by keyword,free1 " \
                  " order by '重要度' desc,'頻度合計' desc" \
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
    '''
    处理要保留的关键字和不保留的关键字
    :return:返回处理后的List
    '''
    keepWordList1=[]
    keepWordList2=[]
    keepWordList3=[]
    removeList=[]
    if report_keyword_property_list:
        try:
            for item1 in report_keyword_property_list:
                flag1 = False
                for mark1 in no_need_words1: #["？","?"]
                    if item1[0].find(mark1)!=-1:
                        flag1 = True
                        removeList.append(item1[0])
                        break
                if flag1==False:
                    keepWordList1.append(item1)

            for item2 in keepWordList1:
                flag2 = False
                for mark2 in no_need_words2: #["っ","ぁ","ぃ","ぅ","ぇ","ヶ"]
                    if item2[0].startswith(mark2) or item2[0].endswith(mark2):
                        flag2 = True
                        removeList.append(item2[0])
                        break
                if flag2 == False:
                    keepWordList2.append(item2)

            for item3 in keepWordList2:
                if item3[0] in keep_words1: # ["ccc","CCC"]
                    keepWordList3.append(item3)
                else:
                    if len(item3[0])<3: #长度小于3的词加入到List
                        keepWordList3.append(item3)
                    elif len(item3[0])>=3: #长度大于3的词
                        str_repeat_list = [everyChar for everyChar in item3[0]] #把词中的每一个字放到str_repeat_list
                        str_repeat_list = list(set(str_repeat_list)) #利用set特性去重
                        if len(str_repeat_list)>1: #不全是同一个字符则加入到List中
                            keepWordList3.append(item3)
                        else:
                            removeList.append(item3[0])
            return keepWordList3
        except Exception as ex:
            logger.error("Call method get_data_from_report_keyword_property() error!")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("Call method no_need_keyword_remove() error!Report_keyword_property_list can't be null.")
        return


def calculate_average(paraList,index):
    '''
    计算某一列的平均值
    :param paraList:要处理的List
    :param index:某一列的下标的值
    :return: 平均值
    '''
    if paraList:
        try:
            df = pd.DataFrame(paraList)
            average_value = df.iloc[:, index].mean()
            return average_value
        except Exception as ex:
            logger.error("Call method calculate_average() error!")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("Call method calculate_average() error!ParaList can't be null.")
        return


def calculate_standard_deviation(paraList,index):
    '''
    计算某一列的标准差
    :param paraList:要处理的List
    :param index:某一列的下标的值
    :return:标准差
    '''
    if paraList:
        try:
            df = pd.DataFrame(paraList)
            colum_value = df.iloc[:, index]
            standard_deviation = np.std(colum_value, ddof=1)
            return standard_deviation
        except Exception as ex:
            logger.error("Call method calculate_standard_deviation() error!")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("Call method calculate_standard_deviation() error!ParaList can't be null.")
        return


def set_generate_year_generate_week(now_year,now_week):
    '''
    设置生成年、生成周.如果能从配置文件读到生成年、生成周，则使用配置文件的生成年和生成周.
    如果配置文件的生成年为空或者配置文件的生成周为空，则使用系统当前时间的前1周的日期作为生成年和生成周.
    :param now_year:系统当前时间所对应的年
    :param now_week:系统当前时间所对应的周
    '''
    global generate_year
    global generate_week
    if not generate_year or not generate_week:
        year_week = "%s-W%s" % (now_year, now_week)
        year_month_day = datetime.datetime.strptime(year_week + '-0', "%Y-W%W-%w")
        generate_year = (year_month_day - datetime.timedelta(weeks=int(1))).strftime('%Y')
        generate_week = (year_month_day - datetime.timedelta(weeks=int(1))).strftime('%V')


def calculate_frequency_deviation_value():
    '''
    计算頻度偏差値
    :return:追加完頻度偏差値后的List
    '''
    if report_keyword_property_list:
        deviation_constant1 = 50
        deviation_constant2 = 10
        for i in range(len(report_keyword_property_list)):
            keyword_frequency_deviation = deviation_constant1 + (report_keyword_property_list[i][2] - keyword_frequency_avg) / keyword_frequency_offet * deviation_constant2
            report_keyword_property_list[i].append(keyword_frequency_deviation)
        return report_keyword_property_list
    else:
        logger.error("Call method calculate_frequency_deviation_value() error!Report_keyword_property_list can't be null.")
        return


def delete_data_from_importance_frequency_deviation():
    '''
    从"report_importance_frequency"表，删除生成年、生成周对应的数据
    '''
    try:
        sql = ' delete from report_importance_frequency where report_year = %s and report_week = %s' \
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
    '''
    插入数据到表"report_importance_frequency"
    :return:增加了生成年、生成周后的List
    '''
    if report_keyword_property_list:
        try:
            insert_importance_frequency_deviation_list = [tuple([generate_year, generate_week, *item]) for item in report_keyword_property_list]
            sql = ' insert into report_importance_frequency (report_year, report_week, keyword, property, keyword_frequency	, importance_degree, keyword_frequency_offet) ' \
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
    else:
        logger.error("Call method insert_into_importance_frequency_deviation() error!Report_keyword_property_list can't be null.")
        return


def calculate_Intercept_X_Variable(para_list):
    '''
    计算Intercept,X_Variable_1,R_Square,Significance_F
    :param para_list:要处理的List
    :return:Intercept,X_Variable_1,R_Square,Significance_F
    '''
    if para_list:
        try:
            df = pd.DataFrame(para_list)
            X = df.iloc[:, 5]
            y = df.iloc[:, 6]
            X1 = X.values.reshape(-1, 1)
            y1 = y.values.reshape(-1, 1)
            clf = LinearRegression()
            clf.fit(X1, y1)
            yhat = clf.predict(X1)
            para_Intercept = clf.intercept_[0]
            para_X_Variable_1 = clf.coef_[0][0]
            SS_Residual = sum((y1 - yhat) ** 2)
            SS_Total = sum((y1 - np.mean(y1)) ** 2)
            para_R_Square = 1 - (float(SS_Residual)) / SS_Total
            adjusted_r_squared = 1 - (1 - para_R_Square) * (len(y1) - 1) / (len(y1) - X1.shape[1] - 1)
            # para_a = linregress(X, y)
            para_a = linregress(X.astype(float), y)
            para_Significance_F = para_a[3]
            return para_Intercept,para_X_Variable_1,para_R_Square[0],para_Significance_F
        except Exception as ex:
            logger.error("Call method calculate_Intercept_X_Variable() error!")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("Call method calculate_Intercept_X_Variable() error!Para_list can't be null.")
        return


def delete_data_from_importance_classification():
    '''
    从"重要度分類"表，删除生成年、生成周对应的数据
    '''
    try:
        sql = ' delete from report_importance_classification where report_year = %s and report_week = %s' \
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
    '''
    生成年、周、关键字、词性的List
    :return: 生成年、生成周、关键字、词性的List
    '''
    if report_keyword_property_list:
        property_list = []
        temp_importance_classification_list = report_keyword_property_list.copy()
        for item in temp_importance_classification_list:
            property_list.append([generate_year,generate_week,item[0],item[1]])
        return property_list
    else:
        logger.error("Call method generate_year_week_keyword_property_list() error!Report_keyword_property_list can't be null.")
        return


def calculate_importance_classification_value(property_list):
    '''
    计算重要度分类
    :param property_list:要处理的List
    :return:增加了重要度分类后的List
    '''
    if property_list:
        importance_classification_list = property_list.copy()
        if len(importance_classification_list) == len(report_keyword_property_list):
            for i in range(len(report_keyword_property_list)):
                importance_degree_g = float(report_keyword_property_list[i][3])*X_Variable_1 + Coefficients_Intercept
                importance_classification_list[i].append(importance_degree_g)
            return importance_classification_list
    else:
        logger.error("Call method calculate_importance_classification_value() error!Property_list can't be null.")
        return


def insert_into_importance_classification(importance_classification_list):
    '''
    插入到表"report_importance_classification"
    :param importance_classification_list:要处理的List
    '''
    if importance_classification_list:
        importance_classification_list = [tuple(item) for item in importance_classification_list ]
        try:
            sql = ' insert into report_importance_classification (report_year, report_week, keyword, property, importance_degree_g) ' \
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
    else:
        logger.error("Call method insert_into_importance_classification() error!Importance_classification_list can't be null.")
        return


def calculate_adjustment(importance_classification_list):
    '''
    计算調整引数(常量)
    :param importance_classification_list:
    :return:調整引数(常量)
    '''
    if importance_classification_list:
        try:
            df = pd.DataFrame(importance_classification_list)
            X = df.iloc[:, 4]
            max_value=max(X)
            calculate_result = (max_value - importance_degree_g_avg)*importance_degree_g_offet
            return calculate_result
        except Exception as ex:
            logger.error("Call method calculate_adjustment() error!")
            logger.error("Exception:" + str(ex))
            raise ex
    else:
        logger.error("Call method calculate_adjustment() error!Importance_classification_list can't be null.")
        return


def calculate_pn_value(importance_classification_list):
    '''
    计算ネガポジ
    :param importance_classification_list:要处理的List
    :return:增加了ネガポジ后的List
    '''
    if importance_classification_list:
        keyword_pn_list = importance_classification_list.copy()
        if len(keyword_pn_list) == len(report_keyword_property_list):
            for i in range(len(keyword_pn_list)):
                pn = (keyword_pn_list[i][4] - importance_degree_g_avg)/adjustment*importance_degree_g_offet
                keyword_pn_list[i].append(pn)
            for item in keyword_pn_list:
                del item[4]
            return keyword_pn_list
    else:
        logger.error("Call method calculate_pn_value() error!Importance_classification_list can't be null.")
        return


def delete_data_from_pn_dictionary():
    '''
    从"report_negative_positive_dict"表，删除生成年、生成周对应的数据
    '''
    try:
        sql = ' delete from report_negative_positive_dict where report_year = %s and report_week = %s' \
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
    '''
    插入到表"report_negative_positive_dict"
    :param pn_list:要处理的List
    '''
    if pn_list:
        keyword_pn_list = [tuple(item) for item in pn_list ]
        try:
            sql = ' insert into report_negative_positive_dict (report_year, report_week, keyword, property, pn) ' \
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
    else:
        logger.error("Call method insert_into_pn_dictionary() error!Pn_list can't be null.")
        return



def calculate_negative_positive_value(set_year,set_week):
    '''
    计算"ネガポジ_個人別"的数据
    :param set_year:生成年
    :param set_week:生成周
    '''
    if set_year and set_week:
        try:
            sql = " select t1.report_year as '年', " \
                  " t1.report_week as '週', " \
                  " t1.employee_code as '社員番号', " \
                  " SUM(CASE WHEN  t2.pn<0 THEN  t2.pn ELSE 0 END) AS 'ネガ合計', " \
                  " SUM(CASE WHEN t2.pn>0 THEN t2.pn ELSE 0 END) AS 'ポジ合計' " \
                  " from report_keyword_property t1 inner join report_negative_positive_dict t2 on t1.keyword=t2.keyword and t1.free1=t2.property " \
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
    else:
        logger.error("Call method calculate_negative_positive_value() error!Set_year and set_week can't be null.")
        return


def delete_data_from_employee_negative_positive():
    '''
    从"ネガポジ_個人別"表，删除生成年、生成周对应的数据
    '''
    try:
        sql = ' delete from report_negative_positive_personal where report_year = %s and report_week = %s' \
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
    '''
    插入到表"report_negative_positive_personal"
    :param employee_negative_positive_list:要处理的List
    '''
    if employee_negative_positive_list:
        keyword_pn_list = [tuple(item) for item in employee_negative_positive_list ]
        try:
            sql = ' insert into report_negative_positive_personal (report_year, report_week, employeecode, negative, positive) ' \
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
    else:
        logger.error("Call method insert_into_employee_negative_positive() error!Employee_negative_positive_list can't be null.")
        return


def delete_data_from_parameter():
    '''
    从"report_parameter"表，删除生成年、生成周对应的数据
    '''
    try:
        sql = ' delete from report_parameter where report_year = %s and report_week = %s' \
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


def insert_into_parameter(set_year,set_week,para_keyword_frequency_avg,para_keyword_frequency_offet,para_importance_degree_g_avg,para_importance_degree_g_offet,para_adjustment,para_R_Square,para_Significance_F,para_Coefficients_Intercept,para_X_Variable_1):
    '''
    :param set_year:生成年
    :param set_week:生成周
    :param para_keyword_frequency_avg:頻度平均
    :param para_keyword_frequency_offet:頻度標準偏差
    :param para_importance_degree_g_avg:重要度平均
    :param para_importance_degree_g_offet:重要度分類標準偏差
    :param para_adjustment:調整引数(常量)
    :param para_R_Square:R_Square
    :param para_Significance_F:Significance_F
    :param para_Coefficients_Intercept:切片
    :param para_X_Variable_1:X
    '''
    try:
        sql = ' insert into report_parameter (report_year, report_week, keyword_frequency_avg, keyword_frequency_offet, importance_degree_g_avg, importance_degree_g_offet, adjustment, R_Square, Significance, Coefficients_Intercept, Coefficients_X_Variable_1) ' \
              ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ' \
              % (set_year,set_week,para_keyword_frequency_avg,para_keyword_frequency_offet,para_importance_degree_g_avg,para_importance_degree_g_offet,para_adjustment,para_R_Square,para_Significance_F,para_Coefficients_Intercept,para_X_Variable_1)
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
    getConn()#数据库连接对象
    current_date = datetime.datetime.now().strftime("%Y-%m-%d") #系统当前日期
    current_year,current_week = get_year_week_from_Mst_date(current_date) #从Mst_date获取当前年和周
    read_dateConfig_file_set_year_week()  # 读配置文件设置参数
    set_generate_year_generate_week(current_year,current_week) #设置generate_year和generate_week,如果generate_year为空或者generate_week为空,则取当前日期对应的年和周前一周所在的年和周
    report_keyword_property_list = get_report_keyword_property_list() #从数据库取出指定年周的数据
    report_keyword_property_list = no_need_keyword_remove() #去掉没有用的关键字
    keyword_frequency_avg = calculate_average(report_keyword_property_list,2) #用"頻度合計"计算"頻度平均"
    keyword_frequency_offet = calculate_standard_deviation(report_keyword_property_list,2) #用"頻度合計"计算"頻度標準偏差"
    report_keyword_property_list = calculate_frequency_deviation_value() #頻度偏差値=50+(某一列的頻度合計-頻度平均)/頻度標準偏差*10,计算出这个值后加入到report_keyword_property_list中
    delete_data_from_importance_frequency_deviation() #插入到"report_importance_frequency"前先删除数据
    list_for_calculate_Coefficients_Intercept_X_Variable_1=insert_into_importance_frequency_deviation() #插入到表"report_importance_frequency",字段"提出年"、"週"、"キーワード"、"詞性"、"頻度"、"重要度"、"頻度偏差値"
    Coefficients_Intercept,X_Variable_1,R_Square,Significance_F = calculate_Intercept_X_Variable(list_for_calculate_Coefficients_Intercept_X_Variable_1) #用"頻度偏差値"和"重要度"做回帰分析,计算出"Intercept, X_Variable_1, R_Square, Significance_F"
    year_week_keyword_property_list = generate_year_week_keyword_property_list() #生成年、周、关键字、词性的List
    report_importance_classification_list = calculate_importance_classification_value(year_week_keyword_property_list) #重要度分类=重要度*X+切片,计算出这个值后加入到report_importance_classification_list中
    delete_data_from_importance_classification()  #插入到表"report_importance_classification"前删除数据
    insert_into_importance_classification(report_importance_classification_list) #插入到表"report_importance_classification",字段"提出年"、"週"、"キーワード"、"詞性"、"report_importance_classification"
    importance_degree_g_avg = calculate_average(report_importance_classification_list,4) #用"重要度分類"计算"重要度平均",通常是50
    importance_degree_g_offet = calculate_standard_deviation(report_importance_classification_list,4) #用"重要度分類"计算"重要度分類標準偏差"
    adjustment = calculate_adjustment(report_importance_classification_list) #计算調整引数(常量),調整引数(常量)=(重要度分類最大值-重要度平均)*重要度分類標準偏差
    report_keyword_pn_list = calculate_pn_value(report_importance_classification_list) #ネガポジ值=(重要度分類 - 重要度平均)/计算調整引数(常量)*重要度分類標準偏差,计算出这个值后加入到report_keyword_pn_list中
    delete_data_from_pn_dictionary()  #插入到表"report_negative_positive_dict"前删除数据
    insert_into_pn_dictionary(report_keyword_pn_list) #插入到表"report_negative_positive_dict",字段"提出年"、"週"、"キーワード"、"詞性"、ネガポジ値"
    year_week_employee_negative_positive_list = calculate_negative_positive_value(generate_year,generate_week) #用生成的字典计算ネガポジ_個人別
    delete_data_from_employee_negative_positive() #插入到表"report_negative_positive_personal"前删除数据
    insert_into_employee_negative_positive(year_week_employee_negative_positive_list) #插入到表"重要度分類",字段"提出年"、"週"、"社員番号"、"キーワード"、"ネガ値"、"ポジ値"
    delete_data_from_parameter() #插入到表"report_parameter"前删除数据
    insert_into_parameter(generate_year,generate_week,keyword_frequency_avg,keyword_frequency_offet,importance_degree_g_avg,importance_degree_g_offet,adjustment,R_Square,Significance_F,Coefficients_Intercept,X_Variable_1) #插入到表"report_parameter",字段"提出年"、"週"、"頻度平均"、"頻度標準偏差"、"重要度分類平均"、"重要度分類標準偏差"、"調整引数"、"Coefficients_Intercept"、"Coefficients_X_Variable_1"
    logger.info("start year week:" + str_start_year_week)
    logger.info("end year week:" + str_end_year_week)
    logger.info("affiliated_company:" + affiliated_company)
    logger.info("continue_weeks:" + continue_weeks)
    logger.info("generate_year:" + generate_year)
    logger.info("generate_week:" + generate_week)
    closeConn()
    time_end = datetime.datetime.now()
    end = time.clock()
    logger.info("Program end,now time is:"+str(time_end))
    logger.info("Program run : %f seconds" % (end - start))



