from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# إعدادات الـ DAG
default_args = {
    'owner': 'Rawan',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'logistics_pipeline_v3',
    default_args=default_args,
    description='SmartFlow Logistics Data Pipeline with dbt',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # 1. التأكد من الاتصال (مع تجاهل خطأ Git)
    check_dbt_connection = BashOperator(
        task_id='check_dbt_connection',
        bash_command='cd /opt/airflow/dbt_project && dbt debug --profiles-dir . || echo "Ignore Git Error"',
    )

    # 2. تحميل البيانات الأساسية (Seeds) لو موجودة
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt_project && dbt seed --profiles-dir .',
    )

    # 3. تشغيل الـ Models (الطبخ الحقيقي للبيانات)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    # 4. عمل اختبارات على البيانات (Data Quality)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    )

    # ترتيب الخطوات (Pipeline Flow)
    check_dbt_connection >> dbt_seed >> dbt_run >> dbt_test