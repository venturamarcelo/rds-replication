import boto3
import logging
import datetime
import json
import os

from operator import itemgetter

logger = logging.getLogger(name=__name__)
env_level = "INFO"
log_level = logging.INFO if not env_level else env_level
logger.setLevel(log_level)

rds = boto3.client('rds')
kms = boto3.client('kms')
waiter = rds.get_waiter('db_snapshot_available')

DB_IDENTIFIER = os.environ.get("DB_IDENTIFIER")

def rename_current_db(db_identifier):
    """ Rename the database {db_identifier} from {db_identifier}-dev to {db_identifier}-dev-old """
    old_identifier = db_identifier+'-dev'
    new_identifier = old_identifier+'-old'
    response = rds.describe_db_instances(
        Filters=[
            {
                'Name': 'db-instance-id',
                'Values': [old_identifier]
            },
        ]
    )
    if response['DBInstances']:
        print(f"Rename {old_identifier} to {new_identifier}")
        logger.info(f"Rename {old_identifier} to {new_identifier}")
        rds.modify_db_instance(
            DBInstanceIdentifier=old_identifier,
            NewDBInstanceIdentifier=new_identifier,
            ApplyImmediately=True
        )
    return new_identifier


def delete_old_instance(db_identifier):
    """ Delete the database {db_identifier}-dev-old """
    logger.info(f"Deleting old db: {db_identifier}-old")
    print(f"Deleting old db: {db_identifier}-dev-old")
    db_name = db_identifier+"-dev-old"
    rds.delete_db_instance(
        DBInstanceIdentifier=db_name,
        SkipFinalSnapshot=True
    )

def get_kms_id(alias):
    """ From KMS, return the CMK KeyId based on Key Alias """
    response = kms.describe_key(KeyId=f'alias/{alias}')
    return response['KeyMetadata']['KeyId']

def copy_snapshot(db_identifier):
    """ Create a copy from the shared snapshot so we have a new manual snapshot using default KMS RDS Key """
    timestamp = '{:%Y%m%d-%H%M%S}'.format(datetime.datetime.now())
    snapshot_copy = f"{db_identifier}-snapshot-copy-{timestamp}"
    kms_default_cmk = get_kms_id('aws/rds')
    print(f"Find latest snapshot of: {db_identifier}")
    response = rds.describe_db_snapshots(SnapshotType='shared', IncludeShared=True)
    snapshots = [s for s in response['DBSnapshots'] if s['DBInstanceIdentifier'] == db_identifier ]
    sorted_keys = sorted(snapshots, key=itemgetter('SnapshotCreateTime'), reverse=True)
    snapshot_id = sorted_keys[0]['DBSnapshotIdentifier']
    rds.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot_id,
                         TargetDBSnapshotIdentifier=snapshot_copy,
                         KmsKeyId=kms_default_cmk)
    return snapshot_copy

def load_snapshot(db_identifier, snapshot):
    """ Load {snapshot} into a new database called {db_identifier}-dev """
    try:
        max_wait = 600
        max_att = int(max_wait / 5)
        print(f"Wait for snapshot: {snapshot}")
        logger.info("Wait for snapshot: {snapshot}")
        waiter.wait(
            DBInstanceIdentifier=db_identifier,
            DBSnapshotIdentifier=snapshot,
            WaiterConfig={'Delay': 5, 'MaxAttempts': max_att}
        )
        db_name = db_identifier+"-dev"
        inst_class = "db.m4.large"
        logger.info(f"Create new: {db_name} from {snapshot}")
        print(f"Create new: {db_name} from {snapshot}")
        rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=db_name,
            DBSnapshotIdentifier=snapshot,
            DBInstanceClass=inst_class,
            PubliclyAccessible=False,
            AutoMinorVersionUpgrade=True,
            CopyTagsToSnapshot=True,
        )
    except Exception as e:
        logger.warning(e)

def lambda_handler(event, context):
    print(f"Starting to restore {DB_IDENTIFIER} from snapshot...")
    snapshot_copy = copy_snapshot(DB_IDENTIFIER)
    rename_current_db(DB_IDENTIFIER)
    load_snapshot(DB_IDENTIFIER,snapshot_copy)
    delete_old_instance(DB_IDENTIFIER)
