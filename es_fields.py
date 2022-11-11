class ESTypeMapping(object):
    Long = 'long'
    Integer = 'integer'
    Short = 'short'
    Byte = 'byte'
    Double = 'double'
    Float = 'float'
    HalfFloat = 'half_float'
    ScaledFloat = 'scaled_float'
    UnsignedLong = 'unsigned_long'
    Keyword = 'keyword'
    ConstantKeyword = 'constant_keyword'
    Wildcard = 'wildcard'
    Text = 'text'
    Date = 'date'
    IP = 'ip'
    Boolean = 'boolean'
    ESObject = 'object'


class ESBaseField:
    def __init__(self, field_name, field_type, **properties):
        self.field_name = field_name
        self.properties = {'type': field_type}
        self.properties.update(properties)

    def add_property(self, **properties):
        """ 添加字段属性 """
        self.properties.update(properties)

    def get_field(self):
        """ 获取属性结果 """
        return {self.field_name: self.properties}


class ESObjectField(ESBaseField):
    """json 类型"""

    def __init__(self, field_name, **properties):
        super(ESObjectField, self).__init__(field_name, ESTypeMapping.ESObject, **properties)


class LongField(ESBaseField):
    """ 64 位长整型 """

    def __init__(self, field_name, **properties):
        super(LongField, self).__init__(field_name, ESTypeMapping.Long, **properties)


class IntegerField(ESBaseField):
    """ 32位整型 """

    def __init__(self, field_name, **properties):
        super(IntegerField, self).__init__(field_name, ESTypeMapping.Integer, **properties)


class ShortField(ESBaseField):
    """ 16位短整型 """

    def __init__(self, field_name, **properties):
        super(ShortField, self).__init__(field_name, ESTypeMapping.Short, **properties)


class ByteField(ESBaseField):
    """ 字节类型 """

    def __init__(self, field_name, **properties):
        super(ByteField, self).__init__(field_name, ESTypeMapping.Byte, **properties)


class DoubleField(ESBaseField):
    """ 双精度浮点数 """

    def __init__(self, field_name, **properties):
        super(DoubleField, self).__init__(field_name, ESTypeMapping.Double, **properties)


class FloatField(ESBaseField):
    """ 单精度浮点型 """

    def __init__(self, field_name, **properties):
        super(FloatField, self).__init__(field_name, ESTypeMapping.Float, **properties)


class HalfFloatField(ESBaseField):
    """ 半浮点型 """

    def __init__(self, field_name, **properties):
        super(HalfFloatField, self).__init__(field_name, ESTypeMapping.HalfFloat, **properties)


class UnsignedLongField(ESBaseField):
    """ 64位 无符号长整形 """

    def __init__(self, field_name, **properties):
        super(UnsignedLongField, self).__init__(field_name, ESTypeMapping.UnsignedLong, **properties)


class KeywordField(ESBaseField):
    """ 关键字 """

    def __init__(self, field_name, **properties):
        super(KeywordField, self).__init__(field_name, ESTypeMapping.Keyword, **properties)


class ConstantKeywordField(ESBaseField):
    """ 关键字常量 值不变 """

    def __init__(self, field_name, **properties):
        super(ConstantKeywordField, self).__init__(field_name, ESTypeMapping.ConstantKeyword, **properties)


class WildcardField(ESBaseField):
    """ 通配符关键字 完整字段搜索慢 适用于类似日志grep等操作 """

    def __init__(self, field_name, **properties):
        super(WildcardField, self).__init__(field_name, ESTypeMapping.Wildcard, **properties)


class TextField(ESBaseField):
    """ 文本字段 """

    def __init__(self, field_name, **properties):
        super(TextField, self).__init__(field_name, ESTypeMapping.Text, **properties)


class DateField(ESBaseField):
    """ 日期 """

    def __init__(self, field_name, **properties):
        super(DateField, self).__init__(field_name, ESTypeMapping.Date, **properties)


class IPField(ESBaseField):
    """ IP """

    def __init__(self, field_name, **properties):
        super(IPField, self).__init__(field_name, ESTypeMapping.IP, **properties)


class BooleanField(ESBaseField):
    """ 布尔值 """

    def __init__(self, field_name, **properties):
        super(BooleanField, self).__init__(field_name, ESTypeMapping.Boolean, **properties)
