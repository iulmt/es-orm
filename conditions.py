class Condition(dict):
    def __init__(self, *args, **kwargs):
        super(Condition, self).__init__(
            *args, **kwargs
        )

    def __add__(self, other: dict):
        for k, v in other.items():
            self.__setitem__(k, v)
        return self

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = type(self)() + value
        super(Condition, self).__setitem__(key, value)

    def __getattribute__(self, item):
        try:
            return super(
                Condition, self
            ).__getattribute__(item)
        except:  # noqa
            return self.get(item)


class Conditions(object):
    def __init__(self, **kwargs):
        self.conditions = list()
        self.__add__(kwargs)

    def __add__(self, other: dict):
        [self.conditions.append(
            Condition({k: v})
        ) for k, v in other.items()]
        return self

    def __iter__(self):
        yield from self.conditions


class BaseItem(object):
    item_name = None

    def __init__(self, conditions: Conditions):
        if self.item_name is None:
            raise Exception('item name is None')
        self.conditions = conditions

    def __iter__(self):
        yield from map(lambda x: {self.item_name: x}, self.conditions)

    def __call__(self):
        return [item for item in self]


class Term(BaseItem):
    item_name = 'term'


class Match(BaseItem):
    item_name = 'match'


class Range(BaseItem):
    item_name = 'range'


class Wildcard(BaseItem):
    item_name = 'wildcard'


class Exists(BaseItem):
    item_name = 'exists'


class MatchAnd(Match):
    """ 分词以 and 逻辑进行查询；默认Match以 or 逻辑查询 """

    def __iter__(self):
        for item in self.conditions:
            for key, val in item.items():
                yield {self.item_name: {key: {'query': val, 'operator': 'and'}}}


class MatchPhrase(BaseItem):
    """ 精确匹配 必须完全一样才能匹配到 """
    item_name = 'match_phrase'
