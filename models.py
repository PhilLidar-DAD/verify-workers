from playhouse.pool import PooledMySQLDatabase
from playhouse.migrate import MySQLMigrator, migrate
from settings import *
import peewee

# Define database models
MYSQL_DB = peewee.MySQLDatabase(
    DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, charset='latin1',
    threadlocals=True)
# MYSQL_DB = PooledMySQLDatabase(DB_NAME, user=DB_USER, password=DB_PASS,
#                                host=DB_HOST, charset='latin1',
#                                threadlocals=True)


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
    ftp_suggest = peewee.CharField(max_length=512, null=True)
    is_file = peewee.BooleanField(null=True)
    dir_path = peewee.CharField(max_length=512, null=True)
    filename = peewee.CharField(null=True)

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


def migrate03():
    MYSQL_DB.connect()
    migrator = MySQLMigrator(MYSQL_DB)
    with MYSQL_DB.atomic() as txn:
        migrate(
            # migrator.add_column('result', 'ftp_suggest', Result.ftp_suggest),
            migrator.add_column('result', 'is_file', Result.is_file),
        )


def migrate04():
    MYSQL_DB.connect()
    migrator = MySQLMigrator(MYSQL_DB)
    with MYSQL_DB.atomic() as txn:
        migrate(
            migrator.add_column('result', 'dir_path', Result.dir_path),
            migrator.add_column('result', 'filename', Result.filename),
        )


def create_tables():
    MYSQL_DB.connect()
    with MYSQL_DB.transaction():
        MYSQL_DB.create_tables([Job, Result], True)


if __name__ == "__main__":

    block_name = 'Agno10A_20130529'
    filename = 'pt000001.laz'

    q = Result.raw("""
SELECT *
FROM result
WHERE has_error = %s AND
      is_file = %s AND
      file_server = %s AND
      ftp_suggest IS NULL
LIMIT 1""", False, True, 'ftp01')
    # , filename, block_name)
#       filename = %s
# ORDER BY jaro_winkler_similarity(dir_path, %s)
    for r in q.execute():
        print r.file_path
        print r.is_file
        print r.file_server
