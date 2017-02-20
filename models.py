from settings import *
import peewee
# import playhouse.pool

# Define database models
MYSQL_DB = peewee.MySQLDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
# MYSQL_DB = playhouse.pool.PooledMySQLDatabase(
#     DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


class BaseModel(peewee.Model):

    class Meta:
        database = MYSQL_DB


class Job(BaseModel):
    file_server = peewee.CharField()
    dir_path = peewee.CharField()
    status = peewee.IntegerField(choices=[(0, 'Working'),
                                          (1, 'Done')],
                                 null=True)
    work_expiry = peewee.DateTimeField(null=True)

    class Meta:
        primary_key = peewee.CompositeKey('file_server', 'dir_path')


class Result(BaseModel):
    file_server = peewee.CharField()
    file_path = peewee.CharField()
    file_ext = peewee.CharField()
    file_size = peewee.BigIntegerField()
    is_processed = peewee.BooleanField()
    is_corrupted = peewee.BooleanField(null=True)
    remarks = peewee.CharField(null=True)
    checksum = peewee.CharField()
    last_modified = peewee.BigIntegerField()
    uploaded = peewee.DateTimeField(null=True)

    class Meta:
        primary_key = peewee.CompositeKey('file_server', 'file_path')
