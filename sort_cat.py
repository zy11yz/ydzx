import gensim


def find_item(list_, item):
    return [index for index, m in enumerate(list_) if m == item]

model = gensim.models.Word2Vec.load('/data/home/lidong1/training/channel_model.model')
with open('/data/home/lidong1/training/channel.txt', 'r', encoding='utf-8') as channel:
    channel_list = [i.strip('\n') for i in channel.readlines()]
cat_list = ['美食','社会','休闲','健康','旅行','教育','体育','科技','军事','财经','时尚','科学','旅游','房产','育儿','动漫','情感','两性','互联网','人文','汽车','民生','文化','科技数码','传媒','运势','家居','娱乐','游戏','时政','历史','宗教','职场','公益','国际','移民','星座','彩票']

match = []
for channel in channel_list:
    score = []
    try:
        for cat in cat_list:
            score.append(model.similarity(channel, cat))
    except:
        match.append(channel + '\t' + '无' + '\t' + '0')
        continue
    # print(score)
    match.append(channel + '\t' + cat_list[find_item(score, max(score))[0]] + '\t' + str(max(score)))
with open('/data/home/lidong1/training/match_out.txt', 'w', encoding='utf-8') as out:
    out.write('\n'.join(match))
