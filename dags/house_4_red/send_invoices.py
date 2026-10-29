from pendulum import duration

from airflow.sdk import dag, task
from airflow.providers.ssh.hooks.ssh import SSHHook

SSH_CONN_ID = "ssh_default"

LAUNCH_CMD = (
    "nohup sh -c '/opt/jobs/send_invoices.sh {run_id}; echo $? > /tmp/send_invoices_{run_id}.rc' "
    ">/dev/null 2>&1 & echo $!"
)
CHECK_CMD = (
    "while kill -0 {pid} 2>/dev/null; do sleep 2; done; "
    '[ "$(cat /tmp/send_invoices_{run_id}.rc 2>/dev/null)" = "0" ]'
)


def _ssh_exec(command):
    hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
    with hook.get_conn() as client:
        _, stdout, _ = client.exec_command(command)
        output = stdout.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()
    if exit_status == -1:
        raise ConnectionError("SSH session closed before the remote command returned an exit status.")
    return exit_status, output


@dag(tags=["house_4_red"])
def send_invoices():

    @task(retries=1, retry_delay=duration(seconds=10))
    def send_invoices(run_id=None):
        _, remote_pid = _ssh_exec(LAUNCH_CMD.replace("{run_id}", run_id))
        print(f"Launched remote job with PID {remote_pid}.")

        exit_status, _ = _ssh_exec(CHECK_CMD.replace("{pid}", remote_pid).replace("{run_id}", run_id))
        if exit_status != 0:
            raise RuntimeError(f"Remote job {remote_pid} failed with exit status {exit_status}.")
        print(f"Remote job {remote_pid} finished.")

    send_invoices()


send_invoices()
