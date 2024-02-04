from botocore.exceptions import ClientError


class DynamoDbUtility:

    @staticmethod
    def delete_table(daws, tablename):
        return daws.Table('tablename').delete()

    @staticmethod
    def createTable(daws, tablename, pk='PK', sk='SK', pktype='S', sktype='S', readcapacityunits=5, writecapacityunits=5, billingmode='PROVISIONED'):
        keyschema = [{
                    'AttributeName': pk,
                    'KeyType': 'HASH'
                }]
        if sk is not None:
            keyschema.append({
                'AttributeName': sk,
                'KeyType': 'RANGE'
            })
        attributedefinition = [
             {
                    'AttributeName': pk,
                    'AttributeType': pktype
                }
        ]
        if sk is not None:
            attributedefinition.append({
                    'AttributeName': sk,
                    'AttributeType': sktype
                })
        table = daws.create_table(
            TableName = tablename,
            KeySchema=keyschema,
            AttributeDefinitions=attributedefinition,
            ProvisionedThroughput={
                'ReadCapacityUnits': readcapacityunits,
                'WriteCapacityUnits': writecapacityunits
            },
            BillingMode=billingmode
        )

        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
        return table

    @staticmethod
    def updateIncremental(table, pk, sk, sets, removes, deletes, adds, eav, ean, cond=None, pkname='PK', skname='SK'):
        # print(gamestate.__dict__)
        # table = create_dynamodb_client().Table("game-server")

        query = ""
        if len(adds) > 0:
            query += "ADD " + ", ".join(adds) + " "
        if len(sets) > 0:
            query += "SET " + ", ".join(sets) + " "
        if len(removes) > 0:
            query += "REMOVE " + ", ".join(removes) + " "
        if len(deletes) > 0:
            query += "DELETE " + ", ".join(deletes) + " "

        result = False
        if len(query) > 0:
            result = DynamoDbUtility.updateStateN(table, pk, sk, query, eav, ean, cond, pkname, skname)
        print("result of updatestateincremental is {}".format(result))
        return result

    @staticmethod
    def readValue(table, pk, sk=None, pkname='PK', skname='SK'):
        keys = {pkname:pk}
        if skname is not None:
            keys[skname] = sk
        try:
            response = table.get_item(Key=keys)
        except ClientError as e:
            print("client error")
            print(e.response['Error']['Message'])
            return False
        else:
            if "Item" in response:
                return response["Item"]
            else:
                return False

    @staticmethod
    def updateStateN(table, pk, sk, query, ExpressionAttributeValues, ExpressionAttributeNames,
                     condition_expression=None, pkname='PK', skname='SK'):
        #print("updatestateN called {}".format(query))
        #table = self.table
        # pk = 'ROOM#{}'.format(roomname)
        # sk = 'METADATA#{}'.format(roomname)

        key = {pkname:pk}
        if skname is not None:
            key[skname] = sk
        kwargs = {}
        kwargs['UpdateExpression'] = query
        if condition_expression is not None:
            kwargs['ConditionExpression'] = condition_expression
        kwargs['ReturnValues'] = 'ALL_NEW'
        if len(ExpressionAttributeNames) > 0:
            kwargs['ExpressionAttributeNames'] = ExpressionAttributeNames
        if len(ExpressionAttributeValues) > 0:
            kwargs['ExpressionAttributeValues'] = ExpressionAttributeValues
        try:
             response = table.update_item(
                 Key=key, **kwargs
            )

        except ClientError as e:
            print(e.response['Error']['Message'])
            print(key, query, ExpressionAttributeValues, ExpressionAttributeNames, condition_expression)
            return False
        else:
            # print(response['Attributes']["gamestate"])
            # self.gamestate.fromDict(response['Attributes']["gamestate"])
            # self.gamestate.roomname = roomname
            # print(response)
            # print("in updateStateN")
            #print(response["Attributes"])
            return response["Attributes"]

    @staticmethod
    def storeValue(table, pk, sk, values: dict, pkname='PK', skname='SK', unique=True):
        item = {pkname: pk}
        if skname is not None:
            item[skname] = sk
        for (k, v) in values.items():
            item[k] = v
        conditionexpression = None
        if unique:
            conditionexpression = 'attribute_not_exists({})'.format(pkname)
            if skname is not None:
                conditionexpression += ' AND attribute_not_exists({})'.format(skname)
        kwargs = {'Item': item}
        if conditionexpression is not None:
            kwargs['ConditionExpression'] = conditionexpression
        #print(item)

        try:
            response = table.put_item(
               **kwargs
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            print(conditionexpression)
            # print(response)
            return False

        else:
            return response

    @staticmethod
    def deleteValue(table, pk, sk, ExpressionAttributeValues={}, ExpressionAttributeNames={}, condition_expression=None, pkname='PK', skname='SK'):
        # print(gamestate.__dict__)
        key = {pkname: pk}
        if skname is not None:
            key[skname] = sk

        try:
            if condition_expression is not None:
                response = table.delete_item(
                    Key=key,
                    ExpressionAttributeValues=ExpressionAttributeValues,
                    ExpressionAttributeNames=ExpressionAttributeNames,
                    ConditionExpression=condition_expression,
                    ReturnValues="NONE"

                )
            else:
                response = table.delete_item(
                    Key=key,
                    ReturnValues="NONE"

                )
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False
        else:
            return True
    @staticmethod
    def queryTable(table, ProjectionExpression, ExpressionAttributeNames, KeyConditionExpression, ExpressionAttributeValues=None,  FilterExpression=None, limit=None, index=None ):
        kwargs = {}
        if index is not None:
            kwargs['IndexName'] = index
            kwargs['TableName'] = table.name
        kwargs['ProjectionExpression'] = ProjectionExpression
        kwargs['ExpressionAttributeNames'] = ExpressionAttributeNames
        if KeyConditionExpression is not None:
            kwargs['KeyConditionExpression'] = KeyConditionExpression
        if ExpressionAttributeValues is not None:
            kwargs['ExpressionAttributeValues'] = ExpressionAttributeValues
        if FilterExpression is not None:
            kwargs['FilterExpression'] = FilterExpression
        if limit is not None:
            kwargs['LIMIT'] = limit
        response = table.query(**kwargs)
        return response['Items']

    @staticmethod
    def scanTable(table, FilterExpression, ProjectionExpression, ExpressionAttributeNames):
        pages = []
        scan_kwargs = { }
        if FilterExpression is not None:    scan_kwargs['FilterExpression'] = FilterExpression
        if ProjectionExpression is not None:    scan_kwargs['ProjectionExpression'] = ProjectionExpression
        if ExpressionAttributeNames is not None: scan_kwargs['ExpressionAttributeNames'] = ExpressionAttributeNames
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            pages.append(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        return pages
    @classmethod
    def createGlobalSecondaryIndex(cls, table, indexName,  pkname, skname, pktype, sktype,  nonkeyattributes=None, readcapacityunits=1, writecapacityunits=1, update=False):
        attributeDefinitions = [{'AttributeName':pkname, 'AttributeType': pktype}]
        if skname is not None:
            attributeDefinitions.append({'AttributeName':skname, 'AttributeType': sktype})
        secondaryindexschema = {}
        secondaryindexschema['IndexName'] = indexName
        keyschema = [{'AttributeName':pkname, 'KeyType': 'HASH'}]
        if skname is not None:
            keyschema.append({'AttributeName':skname, 'KeyType': 'RANGE'})
        secondaryindexschema['KeySchema'] = keyschema
        projection = 'ALL'
        if nonkeyattributes is not None:
            try:
                nonkeyattributes.remove(pkname)
                if skname is not None:
                    nonkeyattributes.remove(skname)
            except:
                pass
            if len(nonkeyattributes) > 0:
                projection = 'INCLUDE'
            else:
                projection = 'KEY_ONLY'
        projectionschema = {'ProjectionType': projection}
        if projection == 'INCLUDE':
            projectionschema['NonKeyAttributes'] = nonkeyattributes
        secondaryindexschema['ProvisionedThroughput'] = {
            'ReadCapacityUnits': readcapacityunits,
            'WriteCapacityUnits': writecapacityunits
        }
        secondaryindexschema['Projection'] = projectionschema
        attrs={}
        attrs['GlobalSecondaryIndexUpdates'] = {'Create':secondaryindexschema}
        if update is True:
            attrs['GlobalSecondaryIndexUpdates'] = {'Update': secondaryindexschema}
        attrs['GlobalSecondaryIndexUpdates'] = [attrs['GlobalSecondaryIndexUpdates']]
        attrs['AttributeDefinitions'] = attributeDefinitions
        table.update(**attrs)

    @classmethod
    def createTableWithLocalIndices(cls, daws, tablename, pkname, skname, pktype, sktype, readcapacityunits=1, writecapacityunits=1, localindices=[]):
        attributeDefinitions = [{'AttributeName': pkname, 'AttributeType': pktype}]
        keyschema = [{'AttributeName':pkname, 'KeyType': 'HASH'}]
        lskeyschema = keyschema
        if skname is not None:
            attributeDefinitions.append({'AttributeName': skname, 'AttributeType': sktype})
            keyschema.append({'AttributeName':skname, 'KeyType': 'RANGE'})


        lses = []
        for index in localindices:
            lse = {}
            lse['IndexName'] = 'list_' + index[0]
            lse['KeySchema'] = lskeyschema.copy()
            lse['KeySchema'].append({'AttributeName':index[0], 'KeyType': 'RANGE'})
            attributeDefinitions.append({'AttributeName': index[0], 'AttributeType':index[1] })
            lse['Projection'] = { 'ProjectionType':'ALL'}
            lses.append(lse)
        table = daws.create_table(
            TableName=tablename,
            KeySchema=keyschema,
            AttributeDefinitions=attributeDefinitions,
            LocalSecondaryIndexes=lses,

            ProvisionedThroughput={
                'ReadCapacityUnits': readcapacityunits,
                'WriteCapacityUnits': writecapacityunits
            }
        )

        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=tablename)

    @classmethod
    def deleteGlobalSecondaryIndex(cls, table, indexname):
        attrs = {}
        attrs['GlobalSecondaryIndexUpdates'] = {'Delete': {'IndexName': indexname}}
        table.update(**attrs)








