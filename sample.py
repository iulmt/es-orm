import datetime, os, logging

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from es_fields import TextField, IntegerField, DateField, ESObjectField
from helper import rdm_str

from sentence import Q, Update
from simple_es_client import SimpleESClient

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s] %(message)s"
)

# 加载ES配置
logging.info('load .env')
load_dotenv()

# 定义 index
logging.info('-> set indices')
indices = {
    'person': [
        TextField('name'),
        IntegerField('age'),
        IntegerField('sex'),
        ESObjectField('life'),
        DateField('birth_day'),
    ]
}

# 创建ES链接
logging.info('-> create es connection')
ES_INSTANCE = Elasticsearch(
    [os.environ['ES_HOST']],
    http_auth=[
        os.environ['AUTHUSER'],
        os.environ['PASSWORD']
    ],
    port=int(os.environ['ES_PORT']),
    timeout=999, max_retries=3, retry_on_timeout=True
)

# 初始化ES客户端
logging.info('-> init es client')
client = SimpleESClient(ES_INSTANCE)

# 初始化 index
logging.info('-> init indices')
client.migrate(indices)

# 插入数据
logging.info('-> insert demo data')
q = Q.filter('term', age=1)
if not client.exists(index='person', body=q()):
    for i in range(1, 101):
        client.insert(index='person', body={
            'age': i,
            'sex': i % 2,
            'name': (rdm_str(i) * 10)[:10],
            'birth_day': datetime.datetime.now(),
            'life': {'style': rdm_str(50)}
        })

# 搜索 年龄等于1的人物信息
q = Q.must('term', age=1)
logging.info(f'-> search age=1: {q()}')
resp = client.search(index='person', body=q())
for p in resp:
    logging.info(p)

# 更新 age=1 任务信息的名称(name)
client.update_by_query(index='person', body=q(), data={'name': 'xiaoming'})
res = client.search(index='person', body=q())
print(res.hits()[0])

# client.insert(index='site', body={'scheme': 'http', 'domain': 'baidu.com', 'path': 'flag/1'})

# query = Q.filter('term', domain='baidu.com')
# resp = client.search(index='site', body=query())
# for item in resp:
#     print(item)

# query = Q.filter('match_phrase', _id='8c005617deee328402f74ae866454cfb')
# resp = client.search(index='site', body=query())
# for item in resp:
#     print(item)
# client.update_by_query(index='site', body=query(), data={'path': '/'})


# query = Q.filter('range', pulled_at={'from': datetime.datetime.now() - datetime.timedelta(days=1)})
# resp = client.search(index='site', body=query())
# for item in resp:
#     print(item)
# print(resp.total())
