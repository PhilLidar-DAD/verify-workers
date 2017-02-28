from playhouse.pool import PooledMySQLDatabase
from playhouse.migrate import MySQLMigrator, migrate
from settings import *
import peewee

# Define database models
# MYSQL_DB = peewee.MySQLDatabase(
#     DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
MYSQL_DB = PooledMySQLDatabase(DB_NAME, user=DB_USER, password=DB_PASS,
                               host=DB_HOST, charset='latin1')


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
    is_dir = peewee.BooleanField(null=True)

    class Meta:
        primary_key = peewee.CompositeKey('file_server', 'dir_path')


class Result(BaseModel):
    file_server = peewee.CharField()
    file_path = peewee.CharField(max_length=512)
    file_ext = peewee.CharField()
    file_type = peewee.CharField(null=True)
    file_size = peewee.BigIntegerField()
    is_processed = peewee.BooleanField()
    has_error = peewee.BooleanField(null=True)
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
        migrate(
            migrator.add_column('job', 'is_dir', Job.is_dir),
        )


def create_tables():
    MYSQL_DB.connect()
    with MYSQL_DB.transaction():
        MYSQL_DB.create_tables([Job, Result], True)


if __name__ == "__main__":
    migrate02()
