from queries import BaseQuery, Should, Must, Filter, MustNot
from conditions import Condition, Conditions, Term, Match, MatchAnd, Range, Exists, MatchPhrase, Wildcard


class Bool(object):
    sen_name = 'bool'

    def __init__(self, *queries):
        self.queries = list(queries)

    def add_query(self, *query: BaseQuery):
        query and self.queries.extend(query)

    @staticmethod
    def parse_query(item: BaseQuery):
        ret = dict()

        for k, v in item().items():
            ret.get(k) and ret[k].extend(v) or ret.update({k: v})

        return ret

    def __call__(self):
        ret = dict()

        for item in self.queries:
            ret.update(self.parse_query(item))
        if 'should' in ret:
            ret['minimum_should_match'] = 1
        return {self.sen_name: ret}


class Sort(object):
    """排序
    sort = Sort(name='desc', age='asc')
    """
    sen_name = 'sort'

    def __init__(self, **kwargs):
        self.sorts = list()

        for key, val in kwargs.items():
            self.sorts.append(Condition({key: val}))

    def __call__(self, *args, **kwargs):
        return {self.sen_name: [item for item in self.sorts]}


class Collapse(object):
    """ 有去重的效果，但是返回的数据总量是去重前的
        通过某个字段折叠数据"""

    sen_name = 'collapse'

    def __init__(self, field: str):
        """ field 去重的字段 """
        self.field = field

    def __call__(self):
        return {self.sen_name: {'field': self.field}}


class Update(object):
    """ 脚本更新 """

    lang = 'painless'
    sen_name = 'script'

    def __init__(self, **params):
        self.params = params

    def __call__(self, source=''):
        for key in self.params:
            source += f'{";" if source else ""}ctx._source.{key}=params.{key}'
        return {self.sen_name: {'source': source, 'params': self.params, 'lang': self.lang}}


class ESPagination(object):
    def __init__(self, page=1, page_size=20, limit=10000):
        self.page = page
        self.limit = limit
        self.page_size = page_size

    def __call__(self):
        from_ = (self.page - 1) * self.page_size
        if from_ >= self.limit:
            return {'size': 0, 'from': self.limit}

        if self.page * self.page_size > self.limit:
            self.page_size = self.limit - from_

        return {'size': self.page_size, 'from': from_}


class Q(object):
    """
    usage:
    # >>> q = Q.filter('exists', field='field_name') # 判断字段是否存在
    # # +/& 号都表示 与操作
    # >>> q += Q.filter('term', field_name='value') # term 查询
    # >>> q &= Q.must_not('match', field_name='value') # match_not 查询
    # # | 表示 或操作
    # >>> q |= Q.filter('term', field_name='value')
    # # 排序 desc 倒叙；asc 顺序
    # >>> sort = Sort(field_name1='asc', field_name2='desc')
    # # 分页
    # >>> pagination = ESPagination()
    # # 根据 指定字段 去重
    # >>> collapse = Collapse('field_name')
    # # 更新 数据
    # >>> updater = Update(field1='value1', field2='value2')
    # # 生成查询语句
    # >>> q(sort=sort, pagination=pagination, collapse=collapse, updater=updater)
    # 范围搜索
    # >>> query = Q.filter('range', pulled_at={'from': datetime.datetime.now() - datetime.timedelta(days=1)})
    """
    sen_name = 'query'

    Q_ITEM_TYPE = {'term': Term, 'match': Match, 'match_and': MatchAnd, 'range': Range, 'exists': Exists,
                   'wildcard': Wildcard, 'match_phrase': MatchPhrase}
    Q_QUERY_TYPE = {'must': Must, 'filter': Filter, 'should': Should, 'must_not': MustNot}

    def __init__(self, *queries):
        self.queries = list(queries)

    def __add__(self, other):
        return self.__and__(other)

    def __and__(self, other):
        if isinstance(other, type(self)):
            self.queries.extend(other.queries)
        elif isinstance(other, BaseQuery):
            self.queries.append(other)
        return self

    def __or__(self, other):
        if isinstance(other, BaseQuery):
            self.queries = [Should(Bool(*self.queries), Bool(other))]
        elif isinstance(other, type(self)):
            self.queries = [Should(Bool(*self.queries), Bool(*other.queries))]
        return self

    def __call__(self,
                 sort: Sort = None,
                 pagination: ESPagination = None,
                 collapse: Collapse = None,
                 updater: Update = None):
        ret = {self.sen_name: Bool(*self.queries)()}
        callable(sort) and ret.update(sort())
        callable(updater) and ret.update(updater())
        callable(collapse) and ret.update(collapse())
        callable(pagination) and ret.update(pagination())
        return ret

    @classmethod
    def common(cls, item_typ, query_typ, **kwargs) -> "Q":
        item = cls.Q_ITEM_TYPE[item_typ](Conditions(**kwargs))
        return cls(cls.Q_QUERY_TYPE[query_typ](item))

    @classmethod
    def must(cls, item_typ, **kwargs) -> "Q":
        return cls.common(item_typ, 'must', **kwargs)

    @classmethod
    def filter(cls, item_typ, **kwargs) -> "Q":
        return cls.common(item_typ, 'filter', **kwargs)

    @classmethod
    def should(cls, item_typ, **kwargs) -> "Q":
        return cls.common(item_typ, 'should', **kwargs)

    @classmethod
    def must_not(cls, item_typ, **kwargs) -> "Q":
        return cls.common(item_typ, 'must_not', **kwargs)


class Result(object):
    from conditions import Condition as Data

    def __init__(self, result: dict):
        self.result = result or {}

    def total(self):
        """数据总条数"""
        return int(self.result.get('hits', {}).get('total', {}).get('value', 0))

    def hits(self):
        """ 获取查询数据列表 """
        return self.result.get('hits', {}).get('hits', [])

    @property
    def scroll_id(self):
        return self.result.get('_scroll_id', '')

    def __iter__(self):
        """ 遍历结果 """
        for item in self.hits():
            yield self.Data(_id=item['_id'], **item['_source'])


__all__ = (
    'Condition', 'Conditions', 'Term', 'Match', 'MatchAnd', 'Range', 'Exists', 'MatchPhrase',
    'Wildcard', 'Should', 'Must', 'Filter', 'MustNot', 'Sort', 'Collapse', 'Update', 'ESPagination',
    'Q', 'Result'
)
# q = Q.filter('match_and', name='xiaoming')
# q |= Q.must('match', age=12)
#
# print(q(collapse=Collapse('name'), updater=Update(name='xiaowang'), sort=Sort(name='desc')))
