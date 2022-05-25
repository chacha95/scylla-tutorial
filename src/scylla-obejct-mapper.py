import os

os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'

import logging

log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from datetime import datetime

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.cqlengine import columns, connection, management
from cassandra.cqlengine.models import Model
from cassandra.policies import (DowngradingConsistencyRetryPolicy,
                                WhiteListRoundRobinPolicy)
from cassandra.query import tuple_factory

from src.config.base import get_settings

settings = get_settings()


class AssetModel(Model):
    __table_name__ = 'Assets'
    asset_id        = columns.Text(primary_key=True)
    description     = columns.Text(required=False)
    created_at      = columns.DateTime()


if __name__ == '__main__':
    hosts = [settings.scylla_host]
    port = settings.scylla_port
    keyspace = settings.scylla_keyspace
    
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(hosts),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=15,
        row_factory=tuple_factory,
    )
    connection.setup(hosts=hosts,
                     execution_profiles={EXEC_PROFILE_DEFAULT: profile}, 
                     port=port, 
                     default_keyspace=keyspace, 
                     protocol_version=4)
 
    log.info("### creating keyspace...")
    management.create_keyspace_network_topology(keyspace, {'replication_factor': 3})
    log.info("### syncing model...")
    management.sync_table(AssetModel)

    # create
    em1 = AssetModel.create(asset_id='em1', description='em1', created_at=datetime.now())
    print(dict(em1))
    em2 = AssetModel.create(asset_id='em2', description='em2', created_at=datetime.now())
    # update => upsert이기에 create구문으로 대체
    AssetModel.create(asset_id='em1', description='em1-updated', created_at=datetime.now())
    # delete
    AssetModel.delete(em2)
    # get
    em1 = AssetModel.get(asset_id='em1')
    print(f'asset_id: {em1.asset_id}, description: {em1.description}, created_at {em1.created_at}')
    q = AssetModel.objects.all()
    print(q.count())

    #### soft delete
    # create
    AssetModel.create(asset_id='em1', description='em1', created_at=datetime.now())
    # read
    AssetModel.get(asset_id='em1')
    # update
    AssetModel.create(asset_id='em1', description='em1-updated', updated_at=datetime.now())
    # delete
    AssetModel.create(asset_id='em1', updated_at=datetime.now())


