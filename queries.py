class BaseQuery(object):
    query_name = None

    def __init__(self, *items):
        if self.query_name is None:
            raise Exception('query name is None')
        self.items = list(items)

    def add_item(self, *items):
        self.items.extend(items)

    def __call__(self):
        ret = list()

        for item in self.items:
            condition = item()
            if isinstance(condition, list):
                ret.extend(condition)
            elif isinstance(condition, dict):
                ret.append(condition)
        return {self.query_name: ret}


class Must(BaseQuery):
    query_name = 'must'


class MustNot(BaseQuery):
    query_name = 'must_not'


class Filter(BaseQuery):
    query_name = 'filter'


class Should(BaseQuery):
    query_name = 'should'
