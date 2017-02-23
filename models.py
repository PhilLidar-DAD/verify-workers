from playhouse.pool import PooledMySQLDatabase
from playhouse.migrate import MySQLMigrator, migrate
from settings import *
import peewee

# Define database models
# MYSQL_DB = peewee.MySQLDatabase(
#     DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
MYSQL_DB = PooledMySQLDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)


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
    file_path_old = peewee.CharField()
    file_path = peewee.TextField(default='')
    file_ext = peewee.CharField()
    file_type = peewee.CharField(null=True)
    file_size = peewee.BigIntegerField()
    is_processed = peewee.BooleanField()
    # is_corrupted = peewee.BooleanField(null=True) -> has_error
    has_error = peewee.BooleanField(null=True)
    remarks_old = peewee.CharField(null=True)
    remarks = peewee.TextField(null=True)
    checksum = peewee.CharField()
    last_modified = peewee.BigIntegerField()
    uploaded = peewee.DateTimeField(null=True)
    processor = peewee.CharField(null=True)

    class Meta:
        primary_key = peewee.CompositeKey('file_server', 'file_path')


def migrate01():
    MYSQL_DB.connect()
    migrator = MySQLMigrator(MYSQL_DB)
    with MYSQL_DB.atomic() as txn:
        migrate(
            migrator.add_column('result', 'file_type', Result.file_type),
            migrator.rename_column('result', 'is_corrupted', 'has_error')
        )


def migrate02():
    MYSQL_DB.connect()
    migrator = MySQLMigrator(MYSQL_DB)
    with MYSQL_DB.atomic() as txn:
        print 'Rename old columns and add new ones...'
        migrate(
            migrator.rename_column('result', 'file_path', 'file_path_old'),
            migrator.rename_column('result', 'remarks', 'remarks_old'),
            migrator.add_column('result', 'file_path', Result.file_path),
            migrator.add_column('result', 'remarks', Result.remarks),
            migrator.add_column('result', 'processor', Result.processor),
        )
        print 'Move data to new columns...'
        for r in Result.select():
            r.file_path = r.file_path_old
            r.remarks = r.remarks_old
            r.save()
        print 'Delete..'
        migrate(
            migrator.drop_column('result', 'file_path_old'),
            migrator.drop_column('result', 'remarks_old'),
        )
