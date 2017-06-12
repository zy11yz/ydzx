import os

'''
1、先查from_id 
api：http://dataplatform.yidian.com:8083/api/other/channel_query/query_all
方法：post
参数：word
2、再请求uid
方法：get
参数：cs_fromid_、cs_keyword_、type
'''

word = input('请输入要查的词(以空格分割)：').split(' ')
path = input('请输入你要存储的路径')
for keyword in word:
    from_id = os.popen("curl -s -d 'word=%s' 'http://dataplatform.yidian.com:8083/api/other/channel_query/query_all'" % word).read().split('-')[0]
    curl = "curl -s http://dataplatform.yidian.com:8083/api/other/uids/query_all\?word\=cs_keyword_%s,cs_fromid_%s\&type\=or > %s/%s.txt;" % (keyword, from_id, path, keyword)
    with open('/data/home/lidong1/User_id_list/script/curl.txt') as c:
        c.write(curl)
    os.system(curl)
