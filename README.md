# ScyllaDB tutorial

## local dev

### set config

```bash
$ cp .env.local .env
```

### setting python 3.9.1

```bash
$ pyenv install 3.9.1
$ PIPENV_VENV_IN_PROJECT=true pipenv install --python 3.9.1
$ pipenv shell
```

### set scylla cluster

```bash
$ docker-compose up -d
```

### check scylladb status

3 노드 모두 UN status인지 확인

```bash
$ docker exec -it scylla-node1 nodetool status
$ docker exec -it scylla-node2 nodetool status
$ docker exec -it scylla-node3 nodetool status
```

### open scylladb cql shell

cql을 입력 할 수 있는 cql shell 접속

```bash
docker exec -it scylla-node1 cqlsh
```

# 현재 구조

`scylla-node1`에 port-forward(19092)를 통해 python clinet가 session을 만들어 통신
RF=3 node=3개인 구조
