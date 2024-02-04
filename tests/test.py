import unittest
from pydynamodb.dynamodbstorable import DynamoDbStorable
from pydynamodb.dynamodbstorable import DynamodbTypes

class MyClass(DynamoDbStorable):
    def __init__(self, myname, age):
        DynamoDbStorable.__init__(self, myname)
        self.age = age
    @classmethod
    def types(cls):
        return {
            'myname': DynamodbTypes.STRING,
            'age': DynamodbTypes.NUMBER
        }
    @classmethod
    def hashname(cls):
        return 'myname'
    @classmethod
    def sortname(cls):
        return None
    @classmethod
    def table_name(cls):
        return 'MyClass'

    @classmethod
    def daws(cls):
        from daws.daws import dawslocal
        return dawslocal
class PyDynamodbTest(unittest.TestCase):
    def setUp(self) -> None:
        #MyClass.createTable()
        pass

    def test_createObj(self):
        n = MyClass("Ranvir", 43)
        n.saveObj()

    def test_updateObj(self):
        obj = MyClass('Ranvir', 43)
        MyClass.updateObj(obj, {'age':45})
    def tearDown(self) -> None:
        #MyClass.delete_table()
        pass
