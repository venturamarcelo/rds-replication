import boto3
import logging
import datetime

from operator import itemgetter

logger = logging.getLogger(name=__name__)
env_level = "INFO"
log_level = logging.INFO if not env_level else env_level
logger.setLevel(log_level)

session = boto3.Session(profile_name='production')
rds = session.client('rds')
waiter = rds.get_waiter('db_snapshot_available')

SHARED_ACCOUNT = ""
DB_IDENTIFIER = "database"
RDS_CUSTOM_KEY_ID = ""

def copy_snapshot(db_identifier):
    timestamp = '{:%Y%m%d-%H%M%S}'.format(datetime.datetime.now())
    snapshot_copy = f"{db_identifier}-snapshot-copy-{timestamp}"
    print(f"Find latest snapshot of: {db_identifier}")
    response = rds.describe_db_snapshots(DBInstanceIdentifier=db_identifier, SnapshotType='automated')
    sorted_keys = sorted(response['DBSnapshots'], key=itemgetter('SnapshotCreateTime'), reverse=True)
    snapshot_id = sorted_keys[0]['DBSnapshotIdentifier']
    rds.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshot_id,
                         TargetDBSnapshotIdentifier=snapshot_copy,
                         KmsKeyId=RDS_CUSTOM_KEY_ID)
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

def main():
    print("Starting...")
    snapshot_copy = copy_snapshot(DB_IDENTIFIER)
    share_snapshot(DB_IDENTIFIER, snapshot_copy)

if __name__ == "__main__":
    main()