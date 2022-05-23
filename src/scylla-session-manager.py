from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import (DowngradingConsistencyRetryPolicy,
                                WhiteListRoundRobinPolicy)
from cassandra.query import tuple_factory


class ScyllaSessionManager:
    def __init__(self, profile: ExecutionProfile, port:int, keyspace: str) -> None:
        self.cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile}, port=port)
        self._set_keyspace(keyspace)
        self.session = self.cluster.connect(keyspace=keyspace)
        self._set_table()

    def _set_keyspace(self, keyspace: str):
        _session = self.cluster.connect()
        _session.execute(
            f"CREATE KEYSPACE {keyspace} "
            "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };"
        )
    
    def _set_table(self):
        self.session.execute(
            f"""
            CREATE TABLE users ( user_id int, fname text, lname text, PRIMARY KEY((user_id)));
            """
        )

    def create_row(self, user_id: int, fname: str, lname: str):
        res = self.session.execute(
            f"""
            INSERT INTO users (user_id, fname, lname)
            VALUES ({user_id}, '{fname}', '{lname}')
            """
        )
        print(res)
    
    def get_row(self, user_id: int):
        row = self.session.execute(
            f"""
            SELECT * FROM users
            WHERE user_id = {user_id}
            """
        )
        print(row.one())
        return 
    
    def delete_row(self, user_id: int):
        self.session.execute(
            f"""
            DELETE FROM users
            WHERE user_id = {user_id}
            """
        )

    def stop(self):
        self.cluster.shutdown()


if __name__ == '__main__':
    hosts = ['127.0.0.1']
    port= 19042
    keyspace = 'test'

    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(hosts),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=15,
        row_factory=tuple_factory,
    )
    ssm = ScyllaSessionManager(profile, port, keyspace)

    # create user
    ssm.create_row(1, 'John', 'Doe')
    # read user
    ssm.get_row(1)
    # delete user
    ssm.delete_row(1)

    ssm.stop()
