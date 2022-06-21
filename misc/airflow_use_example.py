from airflow import models
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount, DriverConfig
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


with models.DAG(
        "reducer_use_example",
        schedule_interval="@once",
        start_date=datetime(2022, 6, 20),
        catchup=False,
        tags=["docker"]
) as dag:

    reducer_a = DockerOperator(
        api_version="1.40",
        docker_url="unix://var/run/docker.sock",
        image="reducer:latest",
        network_mode="bridge",
        cpus=6,
        environment={"ENV REDUCER_RUNNERS_QTY": "18",
                     "ENV REDUCER_LOW_RUNNER": "1",
                     "ENV REDUCER_HIGH_RUNNER": "6",
                     "REDUCER_MODE": "reduce",
                     "LOG_LEVEL": "INFO"},
        mounts=[Mount(source="cifs_data_share",
                      target="/app/inbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"})),
                Mount(source="cifs_data_share",
                      target="/app/outbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"}))
                ],
        tty=True,
        task_id="Reducer_A",
        dag=dag
    )

    reducer_b = DockerOperator(
        api_version="1.40",
        docker_url="unix://var/run/docker.sock",
        image="reducer:latest",
        network_mode="bridge",
        cpus=6,
        environment={"ENV REDUCER_RUNNERS_QTY": "18",
                     "ENV REDUCER_LOW_RUNNER": "7",
                     "ENV REDUCER_HIGH_RUNNER": "12",
                     "REDUCER_MODE": "reduce",
                     "LOG_LEVEL": "INFO"},
        mounts=[Mount(source="cifs_data_share",
                      target="/app/inbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"})),
                Mount(source="cifs_data_share",
                      target="/app/outbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"}))
                ],
        tty=True,
        task_id="Reducer_B",
        dag=dag
    )

    reducer_c = DockerOperator(
        api_version="1.40",
        docker_url="unix://var/run/docker.sock",
        image="reducer:latest",
        network_mode="bridge",
        cpus=6,
        environment={"ENV REDUCER_RUNNERS_QTY": "18",
                     "ENV REDUCER_LOW_RUNNER": "13",
                     "ENV REDUCER_HIGH_RUNNER": "18",
                     "REDUCER_MODE": "reduce",
                     "LOG_LEVEL": "INFO"},
        mounts=[Mount(source="cifs_data_share",
                      target="/app/inbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"})),
                Mount(source="cifs_data_share",
                      target="/app/outbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"}))
                ],
        tty=True,
        task_id="Reducer_C",
        dag=dag
    )

    collector = DockerOperator(
        api_version="1.40",
        docker_url="unix://var/run/docker.sock",
        image="reducer:latest",
        network_mode="bridge",
        cpus=1,
        environment={"ENV REDUCER_RUNNERS_QTY": "18",
                     "ENV REDUCER_LOW_RUNNER": "1",
                     "ENV REDUCER_HIGH_RUNNER": "18",
                     "REDUCER_MODE": "collect",
                     "LOG_LEVEL": "INFO"},
        mounts=[Mount(source="cifs_data_share",
                      target="/app/inbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"})),
                Mount(source="cifs_data_share",
                      target="/app/outbound",
                      no_copy=False,
                      type="volume",
                      driver_config=DriverConfig(
                          name="local",
                          options={"type": "cifs",
                                   "device": "//some.network.location/data_share_name",
                                   "o": "vers=3.0,dir_mode=0777,file_mode=0777,serverino,username=XXX,password=XXX"}))
                ],
        tty=True,
        task_id="Collector",
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    head = DummyOperator(task_id="Start", dag=dag)
    head >> reducer_a >> collector
    head >> reducer_b >> collector
    head >> reducer_c >> collector
