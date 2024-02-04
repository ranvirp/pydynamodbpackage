
from .dynamodbutility import *
from boto3.dynamodb.types import Binary
from decimal import Decimal

from boto3.dynamodb.conditions import Key


from boto3.compat import collections_abc

from botocore.compat import six
class DDBQuery(Key):
    pass
class DynamodbTypes:
    STRING = 'S'
    NUMBER = 'N'
    BINARY = 'B'
    STRING_SET = 'SS'
    NUMBER_SET = 'NS'
    BINARY_SET = 'BS'
    NULL = 'NULL'
    BOOLEAN = 'BOOL'
    MAP = 'M'
    LIST = 'L'
    fnmap = {
        'S':'string',
    'N':'number',
     'B':'binary',
     'SS':'string_set',
     'NS':'number_set',
     'BS':'binary_set',
    'NULL':'null',
     'BOOL':'boolean',
   'M':'map',
     'L':'listlike'
    }

    def check(self, value, valuetype):
        try:
            if valuetype in self.fnmap:
                fn = getattr(self, "_is_" + self.fnmap[valuetype])
                return fn(value)
            else:
                print("valuetype {} not found".format(valuetype))
                return False
        except AttributeError:
            return False

    def _is_null(self, value):
        if value is None:
            return True
        return False

    def _is_boolean(self, value):
        if isinstance(value, bool):
            return True
        return False

    def _is_number(self, value):
        if isinstance(value, (six.integer_types, Decimal)):
            return True
        elif isinstance(value, float):
            raise TypeError(
                'Float types are not supported. Use Decimal types instead.')
        return False

    def _is_string(self, value):
        if isinstance(value, six.string_types):
            return True
        return False

    def _is_binary(self, value):
        if isinstance(value, Binary):
            return True
        elif isinstance(value, bytearray):
            return True
        elif six.PY3 and isinstance(value, six.binary_type):
            return True
        return False

    def _is_set(self, value):
        if isinstance(value, collections_abc.Set):
            return True
        return False

    def _is_type_set(self, value, type_validator):
        if self._is_set(value):
            if False not in map(type_validator, value):
                return True
        return False

    def _is_map(self, value):
        if isinstance(value, collections_abc.Mapping):
            return True
        return False

    def _is_listlike(self, value):
        if isinstance(value, (list, tuple)):
            return True
        return False
    def _is_string_set(self, value):
        return self._is_type_set(value, self._is_string)
    def _is_number_set(self, value):
        return self._is_type_set(value, self._is_number)
    def _is_binary_set(self, value):
        return self._is_type_set(value, self._is_binary)



class DynamoDbStorable:

    def __init__(self, pk, sk=None):
        self.__setattr__(self.hashname(), pk)
        if self.sortname() is not None:
            self.__setattr__(self.sortname(), sk)

    @classmethod
    def createTable(cls):

        pk = cls.hashname()
        sk = cls.sortname()
        pktype = cls._hashtype()
        sktype = cls._sorttype()
        readcapacityunits = cls.read_capacity_units()
        writecapacityunits = cls.write_capacity_units()
        billingmode = cls.billingmode()
        DynamoDbUtility.createTable(cls.daws(), cls.table_name(), pk, sk, pktype, sktype, readcapacityunits, writecapacityunits, billingmode)

    @classmethod
    def createTableWithLocalIndices(cls, indices):
        lindices = []
        for index in indices:
            lindices.append([index, cls.types()[index]])
        pk = cls.hashname()
        sk = cls.sortname()
        pktype = cls._hashtype()
        sktype = cls._sorttype()
        readcapacityunits = cls.read_capacity_units()
        writecapacityunits = cls.write_capacity_units()
        DynamoDbUtility.createTableWithLocalIndices(cls.daws(), cls.table_name(), pk, sk, pktype, sktype, readcapacityunits,
                                    writecapacityunits, localindices=lindices)

    @classmethod
    def delete_table(cls):
        return cls.daws().Table(cls.table_name()).delete()

    @classmethod
    def table_name(cls):
        pass

    @classmethod
    def daws(cls):
        pass
    @classmethod
    def types(cls):
        return {}

    @classmethod
    def hashname(cls):
        return 'PK'
    @classmethod
    def sortname(cls):
        return 'SK'
    @classmethod
    def _hashtype(cls):
        if cls.hashname() in cls.types():
            return cls.types()[cls.hashname()]
        else:
            return 'S'
    @classmethod
    def _sorttype(cls):
        if cls.sortname() in cls.types():
            return cls.types()[cls.sortname()]
        else:
            return 'S'

    def _hashkey(self):
        return self.__getattribute__(self.hashname())

    def _sortkey(self):
        if self.sortname() is not None:
            return self.__dict__[self.sortname()]
        return None
    @classmethod
    def billingmode(cls):
        return 'PROVISIONED'
    @classmethod
    def write_capacity_units(cls):
        return 5
    @classmethod
    def read_capacity_units(cls):
        return 5
    @classmethod
    def _table(cls):
        return cls.daws().Table(cls.table_name())


    def checkCreateConditions(self, valuedict):
        result = True
        errorstring = ''
        typechecker = DynamodbTypes()
        mytypes = self.types()
        for k in valuedict.keys():
            try:
                result1 = typechecker.check(valuedict[k], mytypes[k])
                if not result1:
                    errorstring += 'error in type of {}: required type {} value is {} with type {}\n'.format(k, mytypes[k], valuedict[k], type(valuedict[k]))
                    result = result and result1
            except Exception as e:
                print("{} not found in object".format(k))
                return False

        if result is True:
            return result
        else:
            return errorstring


    def saveObj(self, unique=True):

        pk = self._hashkey()
        sk = self._sortkey()
        dict1 = self._cleanDict()
        '''
        del dict["daws"]
        del dict['tablename']
        del dict['table']
        del dict['pk']
        del dict['sk']
        del dict['pktype']
        del dict['sktype']
        del dict['readcapacityunits']
        del dict['writecapacityunits']
        '''



        response = self.checkCreateConditions(dict1)
        #print(pk, sk)
        if response is True:

            return DynamoDbUtility.storeValue(self._table(), pk, sk, dict1, self.hashname(), self.sortname(), unique=unique)
        else:
            print(response)
            return False

    def deleteObj(self):
        pk = self._hashkey()
        sk = self._sortkey()
        return DynamoDbUtility.deleteValue(self._table(), pk, sk,{},{}, None, self.hashname(), self.sortname())

    def _cleanDict(self):
        dict = self.__dict__.copy()
        '''
        if self.hashname() in dict:
            del dict[self.hashname()]
        if self.sortname() in dict:
            del dict[self.sortname()]
        '''
        return dict

    def readObj(self):
        pk = self._hashkey()
        sk = self._sortkey()

        value = DynamoDbUtility.readValue(self._table(), pk, sk, self.hashname(), self.sortname())

        dict = self._cleanDict()
        if value is not False:
            dict.update(value)
            return dict
        return False

    @classmethod
    def updateObj(cls, self, dictvalue, cond=None):

        response = self.checkCreateConditions(dictvalue)
        if not response:
            return False
        sets = []
        ean = {}
        eav = {}
        for k, v in dictvalue.items():
            if v is None: continue
            sets.append("#{} = :{}".format(k, k))
            ean["#{}".format(k)] = k
            eav[":{}".format(k)] = v

        return DynamoDbUtility.updateIncremental(self._table(), self._hashkey(), self._sortkey(), sets, {}, {}, {}, eav, ean,cond=cond,pkname=self.hashname(), skname=self.sortname())


    @classmethod
    def queryObj(cls, hash_key, cond=None, filter=None, limit=None, index=None):

        projectionexpression = ""
        expressionattributenames = {}
        expressionattributevalues = {}
        keyconditionexpression = None
        FilterExpression = filter
        nonenullvalues = {}
        for k in cls.types():
            projectionexpression += "#{},".format(k)
            expressionattributenames["#{}".format(k)] = k
        if projectionexpression[-1] == ',': projectionexpression = projectionexpression[:-1]
        cond1 = None

        cond1 = Key(cls.hashname()).eq(hash_key)
        if cond is not None:
                cond1 = cond1 & cond

        response = DynamoDbUtility.queryTable(cls._table(), ProjectionExpression=projectionexpression, ExpressionAttributeNames=expressionattributenames, KeyConditionExpression=cond1, limit=limit, FilterExpression=FilterExpression, index=index)
        #print(response)
        '''
        response = cls._table().query(
                ProjectionExpression=projectionexpression,
                ExpressionAttributeNames=expressionattributenames,
                KeyConditionExpression=cond1
            )
        '''

        return response

    @classmethod
    def queryIndex(cls, index, hash_key, cond=None, limit=None):

        projectionexpression = ""
        expressionattributenames = {}
        expressionattributevalues = {}
        keyconditionexpression = None
        FilterExpression = None
        nonenullvalues = {}
        for k in cls.types():
            projectionexpression += "#{},".format(k)
            expressionattributenames["#{}".format(k)] = k
        if projectionexpression[-1] == ',': projectionexpression = projectionexpression[:-1]
        cond1 = None
        secondaryIndex = None
        for si in cls.getSecondaryIndexes():
            if si['IndexName'] == index:
                secondaryIndex = si
                break
        if secondaryIndex is None: return False
        hashname = None
        for keyelement in secondaryIndex['KeySchema']:
            if keyelement['KeyType'] == 'HASH':
                hashname = keyelement['AttributeName']
        if hashname is None: return False
        cond1 = Key(hashname).eq(hash_key)
        if cond is not None:
                cond1 = cond1 & cond

        response = DynamoDbUtility.queryTable(cls._table(), ProjectionExpression=projectionexpression, ExpressionAttributeNames=expressionattributenames, KeyConditionExpression=cond1, limit=limit, index=index)
        #print(response)
        '''
        response = cls._table().query(
                ProjectionExpression=projectionexpression,
                ExpressionAttributeNames=expressionattributenames,
                KeyConditionExpression=cond1
            )
        '''

        return response
    @classmethod
    def getObj(cls, self):
        response = cls.queryObj(self._hashkey())

        if response is not False and len(response) > 0:
            self.__dict__.update(response[0])
            return self
        else:
            return None


    def batchWrite(self, items, deduplicate=True):
        if not deduplicate:
            with self._table().batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            return

        with self._table().batch_writer(overwrite_by_pkeys=[self.hashname(), self.sortname()]) as batch:
            for item in items:
                batch.put_item(Item=item)

    def batchDelete(self, items):
        with self._table().batch_writer() as batch:
            for item in items:
                batch.delete_item(item)
    @classmethod
    def createGlobalSecondaryIndex(cls, indexName, pkname, skname,  nonkeyattributes=None, readcapacityunits=1, writecapacityunits=1):
        table = cls.daws().Table(cls.table_name())
        sktype = None
        if skname is not None:
            sktype = cls.types()[skname]
        DynamoDbUtility.createGlobalSecondaryIndex(table, indexName, pkname, skname, cls.types()[pkname], sktype, nonkeyattributes, readcapacityunits, writecapacityunits
                                                   )
    @classmethod
    def getSecondaryIndexes(cls):
        return cls._table().global_secondary_indexes

    @classmethod
    def scanTable(cls, FilterExpression=None, ProjectionExpression=None, ExpressionAttributeNames=None):
        return DynamoDbUtility.scanTable(cls._table(),FilterExpression, ProjectionExpression, ExpressionAttributeNames)



#if __name__ == "__main__":
    #from rsapi.skillinfo import Prototype
    #k = Prototype("rs")
    #t = k.queryObj()
    #for item in t:
     # print(item.__dict__)
      #item.viewers = []
      #item.saveObj()