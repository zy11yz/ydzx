__author__ = 'Roganic'

import mysql.connector
import os
conn = mysql.connector.connect(user='mkt_data', password='eGRk9Qu^39*3lY7u', host='10.103.32.11', database='mkt_data')
cursor = conn.cursor()


class UserId(object):

    def __init__(self, word_list_1, word_list_2):
        self.w1 = word_list_1
        self.w2 = word_list_2
        self.wl = word_list_1 + word_list_2

    def get_statement(self):  # 将所有关键词进行curl语句的拼接并以dict形式存储
        curl_dict = {}
        for keyword in self.wl:
            sql = "select from_id from dict_channel where channel_name = '%s'" % keyword
            cursor.execute(sql)
            fromid = cursor.fetchall()
            if not fromid:
                wc_curl = "curl -s http://dataplatform.yidian.com:8083/api/other/uids/query_all\?word\=cs_keyword_%s,cs_fromid_\&type\=or|wc -l ;" % (keyword)
                curl = "curl -s http://dataplatform.yidian.com:8083/api/other/uids/query_all\?word\=cs_keyword_%s,cs_fromid_\&type\=or > /data/home/lidong1/User_id_list/%s.txt;" % (keyword, keyword)
            else:
                wc_curl = "curl -s http://dataplatform.yidian.com:8083/api/other/uids/query_all\?word\=cs_keyword_%s,cs_fromid_%s\&type\=or|wc -l ;" % (
                keyword, fromid[0][0])
                curl = "curl -s http://dataplatform.yidian.com:8083/api/other/uids/query_all\?word\=cs_keyword_%s,cs_fromid_%s\&type\=or > /data/home/lidong1/User_id_list/%s.txt ;" % (keyword, fromid[0][0], keyword)
            curl_dict[keyword] = [wc_curl, curl]
        return curl_dict

    def save_uid(self, cd):  # curl所有词的uid并存储
        os.system('cd User_id_list/script; rm dict_channel.txt')
        os.system(
            'mysql -h10.103.20.24 -uolap -polap -P3308 -e"select * from doc_on.dict_channel" > /data/home/lidong1/User_id_list/script/dict_channel.txt')
        os.system(
            'mysql -h10.103.32.11 -mkt_data -peGRk9Qu^39*3lY7u -P3306 -e"load data infile \'/data/home/lidong1/User_id_list/script/dict_channel.txt\' into table dict_channel ignore 1 lines"')
        for keyword in self.wl:
            os.system(cd[keyword][1])

    def count_uid(self, cd):  # 测所有的word的uid数量
        word_list = []
        for keyword in self.wl:
            word_list.append(keyword + '\t' + str(os.popen(cd[keyword][0]).read()))
        with open('/data/home/lidong1/User_id_list/count.txt', 'a+') as count:
            count.write(''.join(word_list))

    def union(self):  # 取两个关键词uid并集数量
        os.system('cd /data/home/lidong1/User_id_list/sort')
        word_list = []
        for w1 in self.w1:
            for w2 in self.w2:
                os.system('sort %s.txt %s.txt|uniq > %S%Sunion.txt' % (w1, w2, w1, w2))
                wc_union = os.popen('wc -l %s%s.txt' % (w1, w2))
                word_list.append(w1+w2+'\t'+str(wc_union.read()))
        print(word_list)
        with open('/data/home/lidong1/User_id_list/count.txt', 'a+') as count:
            count.write(''.join(word_list))

    def intersection(self):  # 取两个关键词uid交集数量
        os.system('cd /data/home/lidong1/User_id_list/sort')
        word_list = []
        for w1 in self.w1:
            for w2 in self.w2:
                os.system('sort %s.txt %s.txt|uniq -d > %S%Sunion.txt' % (w1, w2, w1, w2))
                wc_union = os.popen('wc -l %s%s.txt' % (w1, w2))
                word_list.append(w1 + w2 + '\t' + str(wc_union.read()))
        print(word_list)
        with open('/data/home/lidong1/User_id_list/count.txt', 'a+') as count:
            count.write(''.join(word_list))

    def complment(self):
        pass

    def main(self):
        cd = self.get_statement()
        while True:
            save = input('是否需要存储uid(y/n):')
            if save in ['y', 'Y', 'n', 'N']:
                break
        while True:
            count = input('是否需要测量uid数量(y/n):')
            if count in ['y', 'Y', 'n', 'N']:
                break
        if not self.w2:
            while True:
                union = input('是否需要计算并集(y/n):')
                if union in ['y', 'Y', 'n', 'N']:
                    break
            while True:
                intersection = input('是否需要计算交集(y/n):')
                if intersection in ['y', 'Y', 'n', 'N']:
                    break
        if save in ['y', 'Y']:
            self.save_uid(cd)
        if count in ['y', 'Y']:
            self.count_uid(cd)
        try:
            if union in ['y', 'Y']:
                self.union()
            if intersection in ['y', 'Y']:
                self.intersection()
        except:
            pass

print('---------------------------------------------------------------------------------------------------------')
print('欢迎使用uid统计与下载脚本v2.0，目前支持功能：根据fromid和关键词提取uid并记录数量、计算两个关键词之间uid的交集与并集')
print('---------------------------------------------------------------------------------------------------------')
word_1 = input('请输入要查的词(以空格分割):').split(' ')
while True:
    is_mix = input('是否需要交叉(y/n):')
    if is_mix == 'y' or is_mix == 'Y':
        word_2 = input('请输入要交叉的词(以空格分割):').split(' ')
        break
    elif is_mix == 'n' or is_mix == 'N':
        word_2 = []
        break
userid = UserId(word_1, word_2)
userid.main()
