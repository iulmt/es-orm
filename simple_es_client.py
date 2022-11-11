import logging
import functools

from typing import Callable, Dict
from sentence import Result
from helper import no_exception
from elasticsearch import Elasticsearch, helpers


def params_check(fn: Callable = None, required: list = None, **defaults):
    """参数检查"""

    def _params_check(f):
        @functools.wraps(f)
        def inner(self, **kwargs):
            local_defaults = dict(defaults)
            local_defaults.update(kwargs)

            for arg in (required or []):
                if local_defaults.get(arg) is None:
                    raise Exception(f'{f.__name__} need param({arg})')

            return callable(f) and f(self, **local_defaults)

        return inner

    return callable(fn) and _params_check(fn) or _params_check


class SimpleESClient(object):
    def __init__(self, es: Elasticsearch):
        self.es: Elasticsearch = es

    @params_check(required=['index', 'body'], request_timeout=999)
    def search(self, **kwargs):
        """搜索"""
        params = {
            'body': kwargs['body'],
            'index': kwargs['index'],
            'request_timeout': kwargs['request_timeout']
        }

        if kwargs.get('_source'):
            params['_source'] = kwargs['_source']

        if kwargs.get('doc_type'):
            params['doc_type'] = kwargs['doc_type']

        return Result(self.es.search(**params))

    @params_check(required=['index', 'body'], refresh=False)
    def insert(self, **kwargs):
        """插入数据 无ID可自动生成ID"""
        params = {
            'body': kwargs['body'],
            'index': kwargs['index'],
            'refresh': kwargs['refresh']
        }

        if kwargs.get('doc_type'):
            params['doc_type'] = kwargs['doc_type']

        if kwargs.get('id'):
            params['id'] = kwargs['id']

        self.es.index(**params)

    @params_check(required=['index', 'body'], threads=5, refresh=False, limit=500)
    def bulk_insert(self, **kwargs):
        """批量插入"""

        @no_exception(default=None)
        def get_action(data: dict):
            return {
                **data,
                '_op_type': 'index',
                '_index': kwargs['index'],
            }

        # 批量提交更新
        actions = list(filter(None, [get_action(d) for d in kwargs['body'] if d]))
        if actions:
            act_num = len(actions)
            chunk_size = act_num // kwargs['threads'] if act_num > kwargs['limit'] else act_num
            for success, info in helpers.parallel_bulk(
                    self.es,
                    actions=actions,
                    chunk_size=chunk_size,
                    refresh=kwargs['refresh'],
                    thread_count=kwargs['threads'],
            ):
                (not success) and logging.error(f'insert error: {info}')

    @params_check(scroll='5m', size=200, limit=1000, required=['src', 'dst', 'filters'])
    def reindex(self, **kwargs):
        """数据迁移"""
        data: Result = Result(self.es.search(
            index=kwargs['src'],
            size=kwargs['size'],
            body=kwargs['filters'],
            scroll=kwargs['scroll'],
        ))

        body = list()
        while data.hits():
            [body.append(s) for s in data]
            if len(body) > kwargs['limit']:
                self.bulk_insert(index=kwargs['dst'], body=body)
                body = list()
            data = Result(self.es.scroll(scroll_id=data.scroll_id, scroll=kwargs['scroll']))
        body and self.bulk_insert(index=kwargs['dst'], body=body)

    @params_check(refresh=False, required=['id', 'index', 'body'])
    def create(self, **kwargs):
        """ 插入数据 必须手动加入ID """
        params = {
            'id': kwargs['id'],
            'body': kwargs['body'],
            'index': kwargs['index'],
            'refresh': kwargs['refresh']
        }

        if kwargs.get('doc_type'):
            params['doc_type'] = kwargs['doc_type']

        self.es.create(**params)

    @params_check(threads=5, refresh=False, limit=500, required=['data', 'index', 'body'])
    def update_by_query(self, **kwargs):
        """ 根据查询更新 """
        if not isinstance(kwargs['data'], dict):
            raise Exception('update_by_query need param(data) is dict')

        @no_exception(default=None)
        def get_action(data):
            return {
                '_id': data['_id'],
                '_op_type': 'update',
                '_index': data['_index'],
                'doc': kwargs['data'],
            }

        result: Result = self.search(_source='_id,_index', **kwargs)
        actions = list(filter(None, [get_action(d) for d in result.hits() if d]))
        if actions:
            act_num = len(actions)
            chunk_size = act_num // kwargs['threads'] if act_num > kwargs['limit'] else act_num
            for success, info in helpers.parallel_bulk(
                    self.es,
                    actions=actions,
                    chunk_size=chunk_size,
                    refresh=kwargs['refresh'],
                    thread_count=kwargs['threads'],
            ):
                (not success) and logging.error(f'insert error: {info}')

    @params_check(required=['body', 'index'], refresh=False, request_timeout=999)
    def update_by_script(self, **kwargs):
        """ 当数据量大时，比较慢 """
        if not (kwargs['body'].get('script') and isinstance(kwargs['body']['script'], dict)):
            raise Exception('update_by_script need param(body.script) is dict')

        params = {
            'body': kwargs['body'],
            'index': kwargs['index'],
            'refresh': kwargs['refresh'],
            'request_timeout': kwargs['request_timeout']
        }

        self.es.update_by_query(**params)

    @params_check(required=['body', 'index'])
    def exists(self, **kwargs):
        """ 是否存在 """
        kwargs['body']['size'] = 0
        return self.search(**kwargs).total() > 0

    def del_index(self, index: str):
        """删除index"""
        if self.es.indices.exists(index):
            self.es.indices.delete(index)

    def create_index(self, index: str, properties: list):
        """创建index"""
        if not self.es.indices.exists(index):
            mappings = {'mappings': {'properties': dict()}}
            properties_ = mappings['mappings']['properties']
            for item in properties:
                properties_.update(item.get_field())
            self.es.indices.create(index=index, body=mappings)

    def add_alias(self, index: str, alias: str, is_write_index=True):
        """给index 添加别名"""
        action = [{
            'add': {'index': index, 'alias': alias, 'is_write_index': is_write_index}
        }]
        self.es.indices.update_aliases(body={'actions': action})

    def migrate(self, indices: Dict[str, list]):
        """
        批量新建index
        indices: index列表
        """
        [self.create_index(i, p) for i, p in indices.items()]
