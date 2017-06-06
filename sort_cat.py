import gensim


def find_item(list_, item):
    return [index for index, m in enumerate(list_) if m == item]

model = gensim.models.Word2Vec.load('/data/home/lidong1/training/channel_model.model')
with open('/data/home/lidong1/training/channel_test.txt', 'r', encoding='utf-8') as channel:
    channel_list = [i.strip('\n') for i in channel.readlines()]
cat_list = ['体育', '健康', '其他', '人文', '科技数码', '家居', '星座', '时尚', '时政', '社会', '两性', '美食', '旅行', '教育', '育儿', '财经', '娱乐', '汽车', '游戏', '职场', '休闲', '房产', '军事', '互联网', '科学探索', '娱乐', '科技', '旅游', '文化', '历史', '动漫', '传媒', '科学', '国际', '民生', '宗教', '彩票', '运势', '公益', '情感', '移民', '美女', '搞笑']

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
