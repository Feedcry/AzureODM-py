"""
    test_Entity_Azure

    test Entity against Azure Service
"""
import pytest
from AzureODM.Entity import Entity
from AzureODM.Fields import *
from AzureODM.QuerySet import Q
from datetime import datetime


class TestEntity(Entity):

    PartitionKey = KeyField()
    RowKey = KeyField()
    boolAttr = BooleanField(required=True)
    strAttr = StringField(required=True)
    floatAttr = FloatField(required=True)
    intAttr = IntField(required=True)
    dateAttr = DateField(required=True)

    metas = {
        'table_name': 'test'
    }


@pytest.mark.azure
class Test_save:

    new_entity = None

    def test_save_new(self, azure_ts, request):
        def fin():
            azure_ts.delete_entity(
                'test',
                'p1',
                'r1'
            )
        request.addfinalizer(fin)
        self.new_entity = TestEntity()
        self.new_entity.PartitionKey = 'p1'
        self.new_entity.RowKey = 'r1'
        self.new_entity.boolAttr = True
        self.new_entity.strAttr = 'lol'
        self.new_entity.floatAttr = 1.11
        self.new_entity.intAttr = 1123
        self.new_entity.dateAttr = datetime(2013, 1, 1, 1, 1, 1)
        assert self.new_entity._is_new is True
        assert self.new_entity._saved_copy == {}
        assert self.new_entity._saved_etag is None
        self.new_entity.save()
        assert self.new_entity._is_new is False
        assert self.new_entity._is_changed is False
        assert self.new_entity._saved_etag is not None
        assert self.new_entity._saved_copy['intAttr'] == 1123
        assert self.new_entity._saved_copy['dateAttr'].isoformat() == \
            '2013-01-01T01:01:01+00:00'

    def test_10_entities(self, azure_ts, request):
        entities = []
        # create ten
        for x in range(0, 9):
            new_entity = TestEntity()
            new_entity.PartitionKey = 'p1'
            new_entity.RowKey = 'r{}'.format(x)
            new_entity.boolAttr = True
            new_entity.strAttr = 'lol'
            new_entity.floatAttr = 1.11
            new_entity.intAttr = 1123
            new_entity.dateAttr = datetime(2013, 1, 1, 1, 1, 1)
            entities.append(new_entity)

        # added to different PartitionKey
        for x in range(0, 9):
            new_entity = TestEntity()
            new_entity.PartitionKey = 'p2'
            new_entity.RowKey = 'r{}'.format(x)
            new_entity.boolAttr = True
            new_entity.strAttr = 'lol'
            new_entity.floatAttr = 1.11
            new_entity.intAttr = 1123
            new_entity.dateAttr = datetime(2013, 1, 1, 1, 1, 1)
            entities.append(new_entity)

        def fin():
            for entity in entities:
                entity.delete(force_delete=True)
        request.addfinalizer(fin)

        for entity in entities:
            entity.save()

        # query 3 of them
        res_entities = TestEntity.select(['strAttr', 'dateAttr']) \
            .where(PartitionKey='p1') \
            .andWhere(RowKey__gt='r2').limit(3).go()
        assert len(res_entities) == 3
        assert res_entities[0].PartitionKey == 'p1'
        assert res_entities[0].strAttr == 'lol'
        assert res_entities[0].dateAttr.isoformat() == \
            '2013-01-01T01:01:01+00:00'
        assert res_entities[0].intAttr is None
        assert res_entities[0].RowKey == 'r3'
        assert res_entities[1].RowKey == 'r4'
        assert res_entities[2].RowKey == 'r5'

        r4 = res_entities[1]
        r4.intAttr = 8765
        r4.save()
        # test merge
        merged_r4 = TestEntity.findOne(partition_key='p1',
                                       row_key='r4',
                                       select='*')
        assert merged_r4.intAttr == 8765
        assert merged_r4.strAttr == 'lol'
        assert merged_r4.floatAttr == 1.11
        assert merged_r4._saved_etag == r4._saved_etag

        # query 2 of them
        res_entities = TestEntity.select(['boolAttr', 'dateAttr']) \
            .where(PartitionKey='p1') \
            .andWhere(RowKey__gt='r5').andWhere(RowKey__lt='r8').limit(3).go()
        assert len(res_entities) == 2
        assert res_entities[0].boolAttr is True
        assert res_entities[0].RowKey == 'r6'
        assert res_entities[1].RowKey == 'r7'

        # query 2 of them using or
        res_entities = TestEntity.select(['boolAttr', 'dateAttr']) \
            .where(PartitionKey='p1') \
            .andWhere(Q(RowKey='r1') | Q(RowKey='r8')).limit(3).go()

        assert len(res_entities) == 2
        assert res_entities[0].RowKey == 'r1'
        assert res_entities[1].RowKey == 'r8'

        # query on two different PartitionKey s
        res_entities = TestEntity.select() \
            .where(Q(PartitionKey='p1') | Q(PartitionKey='p2')) \
            .andWhere(RowKey__ge='r5') \
            .andWhere(RowKey__lt='r7') \
            .go()

        assert len(res_entities) == 4
        res_p1 = [
            e for e in filter(lambda x: x.PartitionKey == 'p1', res_entities)]
        assert len(res_p1) == 2
        assert res_p1[0].RowKey == 'r5'
        assert res_p1[1].RowKey == 'r6'
        res_p2 = [
            e for e in filter(lambda x: x.PartitionKey == 'p2', res_entities)]
        assert res_p2[0].RowKey == 'r5'
        assert res_p2[1].RowKey == 'r6'

    @pytest.mark.slow
    @pytest.mark.unimportant
    def test_multiple_partition_speed(self, azure_ts, request):
        from time import time
        # add 20 entities into 20 partitions
        entities = []
        #insert_batch_time = []
        for i in range(0, 20):
            insert_start = time()
            for x in range(0, 20):
                e_start_time = time()
                new_entity = TestEntity()
                new_entity.PartitionKey = 'p{0:02d}'.format(i)
                new_entity.RowKey = 'r{0:02d}'.format(x)
                new_entity.boolAttr = True
                new_entity.strAttr = 'lol'
                new_entity.floatAttr = 1.11
                new_entity.intAttr = 1123
                new_entity.dateAttr = datetime(2013, 1, 1, 1, 1, 1)
                new_entity.save()
                e_end_time = time()
                print('----- insert #{}-{} in {}'.format(
                    i, x, e_end_time - e_start_time))
                entities.append(new_entity)
            insert_end = time()
            #insert_batch_time.append(insert_end - insert_start)
            print('----- insert #{} time: {}'.format(
                i, insert_end - insert_start))

        def fin():
            d_st = time()
            for entity in entities:
                entity.delete(force_delete=True)
            d_et = time()
            print('--- delete finished at in {}'.format(d_et - d_st))
        request.addfinalizer(fin)

        # test query from 4 partition for 40 objects
        #query_constr_time = time()
        s = TestEntity.select()
        #a = Q(RowKey='r{0:02d}'.format(0))
        #b = Q(RowKey='r{0:02d}'.format(0))
        # for q in range(1, 19):
        #    a = (a | Q(RowKey='r{0:02d}'.format(q)))
        #    b = (b | Q(RowKey='r{0:02d}'.format(q)))
        s.where(Q(PartitionKey='p02') & Q(RowKey__ge='r00')).orWhere(
            Q(PartitionKey='p12') & Q(RowKey__ge='r00'))
        #print('----- query constr time: {}'.format(time() - query_constr_time))
        print('------ raw query: \n {}'.format(s.filter))
        query_start_time = time()

        res = s.go()
        query_end_time = time()
        print('----- query rtn time: {}'.format(
            query_end_time - query_start_time))
        print('----- returned: {} entities'.format(len(res)))
        print('---------------- returned entities -------------')
        for entity in res:
            print('P#{} - R#{}'.format(entity.PartitionKey, entity.RowKey))

        # test query 20 partitions for 40 objects
        s2 = TestEntity.select().where(Q(PartitionKey='p00') & Q(RowKey='r12'))
        s2.orWhere(Q(PartitionKey='p00') & Q(RowKey='r10'))
        for r in range(1, 20):
            s2.orWhere(
                Q(PartitionKey='p{0:02d}'.format(r)) &
                Q(RowKey='r02'))
            s2.orWhere(
                Q(PartitionKey='p{0:02d}'.format(r)) &
                Q(RowKey='r18'))
        print('------ raw query: \n {}'.format(s2.filter))
        q2_start = time()
        res2 = s2.go()
        q2_end = time()
        print('----- query2 rtn time: {}'.format(
            q2_end - q2_start))
        print('----- returned {} entities'.format(len(res2)))
        print('---------------- returned entities -------------')
        for entity in res2:
            print('P#{} - R#{}'.format(entity.PartitionKey, entity.RowKey))

        # test in query
        p_keys = ['p{0:02d}'.format(r) for r in range(0, 20)]
        r_keys = ['r18', 'r02']
        s3 = TestEntity.select() \
            .where(Q(PartitionKey__in=p_keys) & Q(RowKey__in=r_keys))
        print('------ raw query: \n {}'.format(s3.filter))
        q3_start = time()
        res3 = s3.go()
        q3_end = time()
        print('----- query3 rtn time: {}'.format(
            q3_end - q3_start))
        print('----- returned {} entities'.format(len(res3)))
        print('---------------- returned entities -------------')
        for entity in res3:
            print('P#{} - R#{}'.format(entity.PartitionKey, entity.RowKey))
