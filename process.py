# -*- coding: utf-8 -*-

import sys
import os
import time,datetime
__author__ = 'liuyichao'

if __name__ == '__main__':
    para_num = len(sys.argv)
    cmd = sys.argv[1]
    profile_words = ['大类','小类','频道','关键词']
    dim_user_words = ['城市','地域','省份','几线城市','年龄','性别','平台']
    profile_user_2 = ['档次']
    dim_user_mp = {
        '城市':'city',
        '地域':'regin',
        '省份':'province',
        '几线城市':'depth',
        '年龄':'age',
        '性别':'gender',
        '平台':'platform'
    }

    profile_mp = {
        '档次':'dlevel'
    }

    if cmd == 'cal':
        sc = sys.argv[2]
        out = sys.argv[3]
        words=sys.argv[4]
        os.system("hdfs dfs -rmr "+out)
        os.system("hdfs dfs -mkdir -p "+out)
        pday = datetime.datetime.fromtimestamp(time.time()) - datetime.timedelta(days=2)
        pday = "%04d-%02d-%02d" %(pday.year,pday.month,pday.day)
        if len(sys.argv)>5:
            pday = sys.argv[5]
        words=list(words.split(','))
        profile_flag = False
        dim_user_flag = False
        profile_flag_2 = False
        for item in words:
            if item in profile_words:
                profile_flag = True
            if item in dim_user_words:
                dim_user_flag = True
            if item in profile_user_2:
                profile_flag_2 = True
        if profile_flag:
            profile_dir = out + '/profile/'
	    if out.endswith('/'):
                profile_dir = out + 'profile/'
            cmd = "pig -D pig.noSplitCombination=true -D mapreduce.output.fileoutputformat.compress=true -D" \
                  " mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec" \
                  " -param PDAY=%s -param SEGMENT=%s -param OUTPUT=%s -param NTASK=2 " \
                  "segment_profile_aggregator.pig"%(pday,sc,profile_dir)
            print cmd
            os.system(cmd)

        if dim_user_flag:
            dim_user_dir = out + '/dim_user/'
            cmd = "pig -D pig.noSplitCombination=true  -param PDAY=%s -param SEGMENT=%s -param OUTPUT=%s -param" \
                  " NTASK=2 dim_user_count.pig"%(pday,sc,dim_user_dir)
            os.system(cmd)
        if profile_flag_2:
            profile_2_dir = out + '/profile_2/'
            cmd = "pig -D pig.noSplitCombination=true  -param PDAY=%s -param SEGMENT=%s -param OUTPUT=%s -param" \
                  " NTASK=2 profile_user_data_2.pig"%(pday,sc,profile_2_dir)
            os.system(cmd)
    if cmd == 'query':
        file = sys.argv[2]
        words=sys.argv[3]
        num = sys.argv[4]
        channel = sys.argv[5]
        words=list(words.split(','))
        for word in words:
            os.system("echo '"+word+"'")
            if word in profile_words:
                file_profile ='hdfs dfs -cat '+ file+'/profile/'+'part-r-*.bz2 | bzcat |awk -v C='+word+' -v S=' \
                            +channel+' -v T='+num+' -f top_in_profile.awk'
                os.system(file_profile)
            if word in dim_user_words:
                grep_word = dim_user_mp[word]
                dim_user_profile = 'hdfs dfs -cat '+file+'/dim_user/part*|grep '+grep_word+'|grep '+channel+ \
                                            "|sort -n -k 4 -r| awk -F\'\t\' '{print $3,$4}'|head -n "+num
                os.system(dim_user_profile)
            if word in profile_user_2:
                grep_word = profile_mp[word]
                profile_2 = 'hdfs dfs -cat '+file+'/profile_2/part*|grep '+grep_word+'|grep '+channel+ \
                                            "|sort -n -k 4 -r| awk -F\'\t\' '{print $3,$4}'|head -n "+num
                os.system(profile_2)
