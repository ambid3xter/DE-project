from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

K8S_SPARK_NAMESPACE = "de-project"
K8S_CONNECTION_ID = "kubernetes_karpov"
GREENPLUM_ID = "greenplume_karpov"
SPARK_NAME = "submit"


def _build_submit_operator(task_id: str, application_file: str, link_dag):
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_file=application_file,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        do_xcom_push=True,
        dag=link_dag
    )


def _build_sensor(task_id: str, application_name: str, link_dag):
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace=K8S_SPARK_NAMESPACE,
        application_name=application_name,
        kubernetes_conn_id=K8S_CONNECTION_ID,
        attach_log=True,
        dag=link_dag
    )


with DAG(
    dag_id="de-project-aleksej-plehanov-qsb8985-dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["de-project"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1
    submit_task_lineitems = _build_submit_operator(
        task_id=f"{SPARK_NAME}_lineitems",
        application_file='spark_lineitems_app.yaml',
        link_dag=dag
    )

    sensor_task_lineitems = _build_sensor(
        task_id='sensor_lineitems',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SPARK_NAME}_lineitems')['metadata']['name']}}}}",
        link_dag=dag
    )

    lineitems_datamart = SQLExecuteQueryOperator(
        task_id="lineitems_datamart",
        conn_id=GREENPLUM_ID,
        sql=
        """
            DROP EXTERNAL TABLE IF EXISTS "aleksej-plehanov-qsb8985"."lineitems";
            CREATE EXTERNAL TABLE "aleksej-plehanov-qsb8985"."lineitems"(
                L_ORDERKEY BIGINT,
                count BIGINT,
                sum_extendprice FLOAT8,
                mean_discount FLOAT8,
                mean_tax FLOAT8,
                delivery_days FLOAT8,
                A_return_flags BIGINT,
                R_return_flags BIGINT,
                N_return_flags BIGINT
            )
            LOCATION ('pxf://de-project/aleksej-plehanov-qsb8985/lineitems_report?PROFILE=s3:parquet&SERVER=default')
            ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
        """,
        dag=dag
    )

    # 2
    submit_task_orders = _build_submit_operator(
        task_id=f"{SPARK_NAME}_orders",
        application_file='spark_orders_app.yaml',
        link_dag=dag
    )

    sensor_task_orders = _build_sensor(
        task_id='sensor_orders',
        application_name=f"{{{{ task_instance.xcom_pull(task_ids='{SPARK_NAME}_orders')['metadata']['name'] }}}}",
        link_dag=dag
)

    orders_datamart = SQLExecuteQueryOperator(
        task_id="orders_datamart",
        conn_id=GREENPLUM_ID,
        sql=
        """
            DROP EXTERNAL TABLE IF EXISTS "aleksej-plehanov-qsb8985"."orders";
            CREATE EXTERNAL TABLE "aleksej-plehanov-qsb8985"."orders"(
                O_MONTH TEXT,
                N_NAME TEXT,
                O_ORDERPRIORITY TEXT,
                orders_count BIGINT,
                avg_order_price FLOAT8,
                sum_order_price FLOAT8,
                min_order_price FLOAT8,
                max_order_price FLOAT8,
                f_order_status BIGINT,
                o_order_status BIGINT,
                p_order_status BIGINT
            )
            LOCATION ('pxf://de-project/aleksej-plehanov-qsb8985/orders_report?PROFILE=s3:parquet&SERVER=default')
            ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
        """,
        dag=dag
    )

    # 3
    submit_task_customers = _build_submit_operator(
        task_id=f"{SPARK_NAME}_customers",
        application_file='spark_customers_app.yaml',
        link_dag=dag
    )

    sensor_task_customers = _build_sensor(
        task_id='sensor_customers',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SPARK_NAME}_customers')['metadata']['name']}}}}",
        link_dag=dag
    )

    customers_datamart = SQLExecuteQueryOperator(
        task_id="customers_datamart",
        conn_id=GREENPLUM_ID,
        sql=
        """
            DROP EXTERNAL TABLE IF EXISTS "aleksej-plehanov-qsb8985"."customers";
            CREATE EXTERNAL TABLE "aleksej-plehanov-qsb8985"."customers"(
                R_NAME TEXT,
                N_NAME TEXT,
                C_MKTSEGMENT TEXT,
                unique_customers_count BIGINT,
                avg_acctbal FLOAT8,
                mean_acctbal FLOAT8,
                min_acctbal FLOAT8,
                max_acctbal FLOAT8
            )
            LOCATION ('pxf://de-project/aleksej-plehanov-qsb8985/customers_report?PROFILE=s3:parquet&SERVER=default')
            ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
        """,
        dag=dag
    )

    # 4
    submit_task_suppliers = _build_submit_operator(
        task_id=f"{SPARK_NAME}_suppliers",
        application_file='spark_suppliers_app.yaml',
        link_dag=dag
    )

    sensor_task_suppliers = _build_sensor(
        task_id='sensor_suppliers',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SPARK_NAME}_suppliers')['metadata']['name']}}}}",
        link_dag=dag
    )

    suppliers_datamart = SQLExecuteQueryOperator(
        task_id="suppliers_datamart",
        conn_id=GREENPLUM_ID,
        sql=
        """
            DROP EXTERNAL TABLE IF EXISTS "aleksej-plehanov-qsb8985"."suppliers";
            CREATE EXTERNAL TABLE "aleksej-plehanov-qsb8985"."suppliers"(
                R_NAME TEXT,
                N_NAME TEXT,
                unique_supplers_count BIGINT,
                avg_acctbal FLOAT8,
                mean_acctbal FLOAT8,
                min_acctbal FLOAT8,
                max_acctbal FLOAT8
            )
            LOCATION ('pxf://de-project/aleksej-plehanov-qsb8985/suppliers_report?PROFILE=s3:parquet&SERVER=default')
            ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
        """,
        dag=dag
    )

    # 5
    submit_task_parts = _build_submit_operator(
        task_id=f"{SPARK_NAME}_part",
        application_file='spark_part_app.yaml',
        link_dag=dag
    )

    sensor_task_parts = _build_sensor(
        task_id='sensor_part',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SPARK_NAME}_part')['metadata']['name']}}}}",
        link_dag=dag
    )

    parts_datamart = SQLExecuteQueryOperator(
        task_id="part_datamart",
        conn_id=GREENPLUM_ID,
        sql=
        """
            DROP EXTERNAL TABLE IF EXISTS "aleksej-plehanov-qsb8985"."part";
            CREATE EXTERNAL TABLE "aleksej-plehanov-qsb8985"."part"(
                N_NAME TEXT,
                P_TYPE TEXT,
                P_CONTAINER TEXT,
                parts_count BIGINT,
                avg_retailprice FLOAT8,
                size BIGINT,
                mean_retailprice FLOAT8,
                min_retailprice FLOAT8,
                max_retailprice FLOAT8,
                avg_supplycost FLOAT8,
                mean_supplycost FLOAT8,
                min_supplycost FLOAT8,
                max_supplycost FLOAT8
            )
            LOCATION ('pxf://de-project/aleksej-plehanov-qsb8985/parts_report?PROFILE=s3:parquet&SERVER=default')
            ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
        """,
        dag=dag
    )

    start >> [submit_task_lineitems, submit_task_orders, submit_task_customers, submit_task_suppliers, submit_task_parts]

    submit_task_lineitems >> sensor_task_lineitems >> lineitems_datamart >> end
    submit_task_orders >> sensor_task_orders >> orders_datamart >> end
    submit_task_customers >> sensor_task_customers >> customers_datamart >> end
    submit_task_suppliers >> sensor_task_suppliers >> suppliers_datamart >> end
    submit_task_parts >> sensor_task_parts >> parts_datamart >> end
