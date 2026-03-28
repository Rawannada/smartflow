from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# إعدادات الداج الأساسية
default_args = {
    'owner': 'Rawan',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'logistics_pipeline_v3',  # غيرت الاسم لـ v3 عشان نضمن إن الأيرفلو يقرأ النسخة الجديدة
    default_args=default_args,
    description='Run dbt transformations with correct PATH',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
) as dag:

    # 1. اختبار الاتصال (مع تعريف المسار عشان ما يضربش)
    check_dbt = BashOperator(
        task_id='check_dbt_connection',
        bash_command='export PATH=$PATH:/root/.local/bin && cd /opt/airflow/dbt_project && dbt debug --profiles-dir .'
    )

    # 2. تشغيل الموديلز
    run_dbt = BashOperator(
        task_id='dbt_run_models',
        bash_command='export PATH=$PATH:/root/.local/bin && cd /opt/airflow/dbt_project && dbt run --profiles-dir .'
    )

    # 3. عمل الاختبارات
    test_dbt = BashOperator(
        task_id='dbt_test_models',
        bash_command='export PATH=$PATH:/root/.local/bin && cd /opt/airflow/dbt_project && dbt test --profiles-dir .'
    )

    # ترتيب الخطوات
    check_dbt >> run_dbt >> test_dbt