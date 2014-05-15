AzureODM
========

Mongoengine inspired lightweight ODM for Azure Table Service

Requirement
-----------
Azure Python SDK: [https://github.com/Azure/azure-sdk-for-python](https://github.com/Azure/azure-sdk-for-python)

Example Define Entity (Model)
---------------------

```python

from AzureODM.Entity import Entity
from AzureODM.Fields import (
    StringField,
    BooleanField,
    KeyField,
    DateField,
    IntField,
    JSONField)

class BlogPost(Entity):

    #: ``PartitionKey`` for azure table, can use year for partition
    PartitionKey = KeyField()
    #: ``RowKey``  use timestamp for each blog post
    RowKey = KeyField()
    #: page Title (title tag)
    title = StringField(required=True)
    #: store the page description
    content = StringField()
    #: og:type, like article, video
    tags = JSONField()
    #: post category
    category = StringField()

    metas = {
        'table_name': 'posts'
    }

    @classmethod
    def get_by_timestamp(self, timestamp):
        """get the page by url

        :param str url:
        :raises TypeError: if url is not str
        """
        if not isinstance(url, str):
            raise TypeError('url is not a str, {}'.format(url))
        return self.findOne(partition_key=timestamp_to_year(timestamp),
          row_key=timestamp)
```

API Document
------------
[http://feedcry.github.io/AzureODM/docs](http://feedcry.github.io/AzureODM/docs)

Authors
-------

**Chen Liang**

+ http://chen.technology
+ http://github.com/uschen

Credits
-------
+ Inspired by [https://github.com/MongoEngine/mongoengine](https://github.com/MongoEngine/mongoengine)

License
-------
Licensed under the New BSD License
