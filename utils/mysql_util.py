# coding:utf-8
import sys
sys.path.append('/data/projects/job_manage')

import re
import pymysql
import utils.config as config
from utils.log import logger
# def mysql_oper(func):
#     def wrapper(self, *args, **kwargs):
#         res = func(self, *args, **kwargs)
#         return res
#     return wrapper



def mysql_oper(func):
    def wrapper(self, *args, **kwargs):
        if self.conn != None:
            try:
                res = func(self, *args, **kwargs)
                logger.info ('mysql %s success'%func.__name__)
                return res
            except Exception as e:
                # print (e)
                self.conn.rollback()
                logger.error('mysql %s failed'%func.__name__)
                logger.error('error msg is %s' % str(e))

        if func.__name__ != 'select':
            return 0
        else:
            return 0, None, None
    return wrapper



def parse_sql_title(sql_text):
    '''
    解析 sql 字段 select 部分，将数据结果的标题提取出来
    :param sql_text:  sql_text sql语言 请勿使用 select * 字段
    :return: 标题 list
    '''
    # cur.description
    try:
        sql_text = str(sql_text)
    except:
        logger.error('sql_text format error, please check sql_text')
        return None
    try:
        data_title_list_mixed = sql_text.lower().split('from')[0].replace('select', '').replace('\n', '')
    except:
        logger.error ('sql_text state error, please check sql_text')
        return None

    try:
        re_find_all = re.findall('(\(.*?)as', data_title_list_mixed, re.S)
        if re_find_all:
            re_list = sorted(re_find_all, key=lambda i: len(i), reverse=True)
            for re_find_i in re_list:
                if re_find_i:
                    data_title_list_mixed = data_title_list_mixed.replace(re_find_i, '')


        data_title_list_mixed = data_title_list_mixed.split(',')
        data_title_list = [data_title_one.split('as')[-1].strip() if 'as ' in data_title_one else data_title_one.strip() for
                           data_title_one in data_title_list_mixed]
    except:
        logger.error ('code error, please repair')
        return None
    return data_title_list


class MysqlUtil():
    def __init__(self):
    # def __init__(self, user_name, passwd, host, port, db):
        super(MysqlUtil, self).__init__()

        logger.info ('mysql connect start, host is %s'%config.MYSQL_HOST)
        try:
            self.conn = pymysql.connect(
                user=config.MYSQL_USER, password=config.MYSQL_PASSWD, host=config.MYSQL_HOST, port=config.MYSQL_PORT,
                database=config.MYSQL_DATABASE, charset="utf8mb4"
            )
            self.cur = self.conn.cursor()
            logger.info ('mysql connect success')
        except:
            logger.error ('mysql connect failed')
            self.conn = None
            self.cur = None

    @mysql_oper
    def insert(self, sql_state):
        """
        :param sql_state:  '''insert into table_name(col_1, col_2, col_3) values("%s", "%s", "%s")'''%(val_1, val_2, val_3)
        :return: run_flag 1 success  0 failed
        """
        sql_state = sql_state.replace('"null"', 'null').replace("'null'", 'null')
        # print ('mysql insert %s'%sql_state.split('values')[0].replace('\n', '').replace('\t', '').replace('\r', ''))
        logger.info ('mysql insert %s'%sql_state.replace('\n', '').replace('\t', '').replace('\r', ''))
        self.cur.execute(sql_state)
        self.conn.commit()
        return 1

    @mysql_oper
    def insertmany(self, sql_state, value_tuple_list):
        """
        :param sql_state: '''insert into table_name(col_1, col_2, col_3) values(%s, %s, %s)'''
        :param value_tuple_list: [(val_1, val_2, val_3),(val_1, val_2, val_3),(val_1, val_2, val_3)]
        :return: run_flag 1 success  0 failed
        """
        logger.info ('mysql insertmany %s'%sql_state.split('values')[0].replace('\n', '').replace('\t', '').replace('\r', ''))
        self.cur.executemany(sql_state, value_tuple_list)
        self.conn.commit()
        return 1

    @mysql_oper
    def delete(self, sql_state):
        # 删
        """
        '''delete from table_name where col=value;'''
        :param sql_state:
        :return:
        """
        logger.info ('mysql delete %s'%sql_state.replace('\n', '').replace('\t', '').replace('\r', ''))
        rows = self.cur.execute(sql_state)
        self.conn.commit()
        return 1

    @mysql_oper
    def update(self, sql_state):
        # 改
        """
        :param sql_state: '''UPDATE table_name SET col1 = "val_1" WHERE col_2 = "val_2"'''
        :return:
        """
        sql_state = sql_state.replace('"null"', 'null').replace("'null'", 'null')
        logger.info ('mysql update %s'%sql_state.replace('\n', '').replace('\t', '').replace('\r', ''))
        rows = self.cur.execute(sql_state)
        self.conn.commit()
        return 1

    @mysql_oper
    def select(self, sql_state):
        # 查
        """
        '''SELECT col_1, col_2 from table_name;'''
        :param sql_state:
        :return:
        """
        logger.info ('mysql select %s'%sql_state)
        rows = self.cur.execute(sql_state)
        data = self.cur.fetchall()
        self.conn.commit()
        flag = 1
        return flag, rows, data

    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass




if __name__ == '__main__':
    mu = MysqlUtil()

    # user_name = config.MYSQL_USERNAME,
    # passwd = config.MYSQL_PASSWD,
    # host = config.MYSQL_HOST,
    # port = config.MYSQL_PORT,
    # db = config.MYSQL_DEFAULT_DB
    # mu = MysqlUtil(user_name, passwd, host, port, db)

    # mu.select("""select *,aaa from fk_kuanbiao.user_idcard_addr_parse""")
    print ()
