from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sdk import dag, task


@dag(schedule=None)
def missing_connection():

    @task
    def fetch_report():
        hook = SSHHook(ssh_conn_id="reporting_server")
        with hook.get_conn() as client:
            _, stdout, _ = client.exec_command("cat /reports/latest.csv")
            print(stdout.read().decode())

    fetch_report()


missing_connection()
