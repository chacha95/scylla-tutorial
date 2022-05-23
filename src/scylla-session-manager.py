from cassandra import AlreadyExists, ConsistencyLevel
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
        try:
            _session = self.cluster.connect()
            _session.execute(
                f"CREATE KEYSPACE {keyspace} "
                "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 };"
            )
        except AlreadyExists as e:
            print(f'keyspace {keyspace} already exists')
            pass
        except Exception as e:
            print(type(e), e)
    
    def _set_table(self, table='users'):
        try:
            self.session.execute(
                f"""
                CREATE TABLE {table} ( user_id int, fname text, lname text, PRIMARY KEY((user_id)));
                """
            )
        except AlreadyExists as e:
            print(f'table {table} already exists')
            pass
        except Exception as e:
            print(type(e), e)

    def create_row(self, user_id: int, fname: str, lname: str):
        try:
            res = self.session.execute(
            f"""
            INSERT INTO users (user_id, fname, lname)
            VALUES ({user_id}, '{fname}', '{lname}')
            """
        )
        except Exception as e:
            print(e)
    
    def get_row(self, user_id: int):
        try:
            row = self.session.execute(
                f"""
                SELECT * FROM users
                WHERE user_id = {user_id}
                """
            )
        except Exception as e:
            print(e)
        print(row.one())
    
    def delete_row(self, user_id: int):
        try:
            self.session.execute(
                f"""
                DELETE FROM users
                WHERE user_id = {user_id}
                """
            )
        except Exception as e:
            print(e)

    def stop(self):
        try:
            self.cluster.shutdown()
        except Exception as e:
            print(e)


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
