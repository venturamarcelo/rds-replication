import json
import boto3
import logging
import datetime
import os

from operator import itemgetter

logger = logging.getLogger(name=__name__)
env_level = "INFO"
log_level = logging.INFO if not env_level else env_level
logger.setLevel(log_level)

rds = boto3.client('rds')
kms = boto3.client('kms')
waiter = rds.get_waiter('db_snapshot_available')

SHARED_ACCOUNT = os.environ.get("SHARED_ACCOUNT")
DB_IDENTIFIER = os.environ.get("DB_IDENTIFIER")
CMK_ALIAS = os.environ.get("CMK_ALIAS")

def get_kms_id(alias):
    """ From KMS, return the CMK KeyId based on Key Alias """
    response = kms.describe_key(KeyId=f'alias/{alias}')
    return response['KeyMetadata']['KeyId']

def copy_snapshot(db_identifier):
    timestamp = '{:%Y%m%d-%H%M%S}'.format(datetime.datetime.now())
    snapshot_copy = f"{db_identifier}-snapshot-copy-{timestamp}"
    keyid = get_kms_id(CMK_ALIAS)
    print(f"Find latest snapshot of: {db_identifier}")
    response = rds.describe_db_snapshots(DBInstanceIdentifier=db_identifier, SnapshotType='automated')
    sorted_keys = sorted(response['DBSnapshots'], key=itemgetter('SnapshotCreateTime'), reverse=True)
    snapshot_id = sorted_keys[0]['DBSnapshotIdentifier']
    rds.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot_id,
                         TargetDBSnapshotIdentifier=snapshot_copy,
                         KmsKeyId=keyid)
    return snapshot_copy

def share_snapshot(db_identifier, snapshot_id):
    try:
        max_wait = 250
        max_att = int(max_wait / 5)
        print(f"Wait for snapshot: {snapshot_id}")
        logger.info("Wait for snapshot: {snapshot_id}")
        waiter.wait(
            DBInstanceIdentifier=db_identifier,
            DBSnapshotIdentifier=snapshot_id,
            WaiterConfig={'Delay': 5, 'MaxAttempts': max_att}
        )
        print(f"Share snapshot: {snapshot_id} of {db_identifier}")
        logger.info("Share snapshot: {snapshot_id} of {db_identifier}")
        rds.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snapshot_id,
            AttributeName="restore",
            ValuesToAdd=[SHARED_ACCOUNT]
        )
    except Exception as e:
        logger.warning(e)

def lambda_handler(event, context):
    print(f"Starting Copying a new snapshot and sharing with {SHARED_ACCOUNT} account ...")
    snapshot_copy = copy_snapshot(DB_IDENTIFIER)
    share_snapshot(DB_IDENTIFIER, snapshot_copy)
    