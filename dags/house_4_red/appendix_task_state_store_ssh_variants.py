import base64

from pendulum import duration

from airflow.sdk import dag
from airflow.sdk.bases.operator import BaseOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

SSH_CONN_ID = "ssh_default"

LAUNCH_CMD = (
    "nohup sh -c 'sleep 45; rc=$?; echo \"$(date +%s%3N) pid=$$\" >> {out_file}; "
    "echo $rc > {out_file}.rc' >/dev/null 2>&1 & echo $!"
)
CHECK_CMD = (
    "while kill -0 {pid} 2>/dev/null; do sleep 2; done; "
    "rc=$(cat {out_file}.rc 2>/dev/null); cat {out_file}; "
    '[ "$rc" = "0" ]'
)


def _ssh_exec(command):
    hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
    with hook.get_conn() as client:
        _, stdout, _ = client.exec_command(command)
        output = stdout.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()
    return exit_status, output


def _pythonoperator_example(launch_cmd, check_cmd, out_file, **context):
    tss = context["task_state_store"]
    remote_pid = tss.get("remote_pid")

    if remote_pid is None:
        _, remote_pid = _ssh_exec(launch_cmd.replace("{out_file}", out_file))
        tss.set("remote_pid", remote_pid)
        raise RuntimeError(
            f"Simulated worker interruption right after launch. Remote PID {remote_pid} "
            "keeps running; the retry reconnects via the stored PID instead of relaunching."
        )

    print(f"Reconnecting to remote PID {remote_pid} from a previous attempt.")
    exit_status, output = _ssh_exec(
        check_cmd.replace("{pid}", remote_pid).replace("{out_file}", out_file)
    )
    print(f"Remote job output: {output}")
    if exit_status != 0:
        raise RuntimeError(f"Remote job {remote_pid} failed with exit status {exit_status}.")


class CustomBaseOperator(BaseOperator):

    template_fields = ("launch_cmd", "check_cmd", "out_file")

    def __init__(self, *, launch_cmd, check_cmd, out_file, **kwargs):
        super().__init__(**kwargs)
        self.launch_cmd = launch_cmd
        self.check_cmd = check_cmd
        self.out_file = out_file

    def execute(self, context):
        tss = context["task_state_store"]
        remote_pid = tss.get("remote_pid")

        if remote_pid is None:
            _, remote_pid = _ssh_exec(self.launch_cmd.replace("{out_file}", self.out_file))
            tss.set("remote_pid", remote_pid)
            raise RuntimeError(
                f"Simulated worker interruption right after launch. Remote PID {remote_pid} "
                "keeps running; the retry reconnects via the stored PID instead of relaunching."
            )

        self.log.info(f"Reconnecting to remote PID {remote_pid} from a previous attempt.")
        exit_status, output = _ssh_exec(
            self.check_cmd.replace("{pid}", remote_pid).replace("{out_file}", self.out_file)
        )
        self.log.info(f"Remote job output: {output}")
        if exit_status != 0:
            raise RuntimeError(f"Remote job {remote_pid} failed with exit status {exit_status}.")


class CustomSSHOperator(SSHOperator):

    template_fields = ("launch_cmd", "check_cmd", "out_file", *SSHOperator.template_fields)

    def __init__(self, *, launch_cmd, check_cmd, out_file, **kwargs):
        super().__init__(**kwargs)
        self.launch_cmd = launch_cmd
        self.check_cmd = check_cmd
        self.out_file = out_file

    def execute(self, context):
        tss = context["task_state_store"]
        remote_pid = tss.get("remote_pid")

        if remote_pid is None:
            self.command = self.launch_cmd.replace("{out_file}", self.out_file)
            remote_pid = base64.b64decode(super().execute(context)).decode().strip()
            tss.set("remote_pid", remote_pid)
            raise RuntimeError(
                f"Simulated worker interruption right after launch. Remote PID {remote_pid} "
                "keeps running; the retry reconnects via the stored PID instead of relaunching."
            )

        self.log.info(f"Reconnecting to remote PID {remote_pid} from a previous attempt.")
        self.command = self.check_cmd.replace("{pid}", remote_pid).replace("{out_file}", self.out_file)
        return super().execute(context)


@dag(tags=["house_4_red", "appendix"])
def task_state_store_ssh_variants():

    PythonOperator(
        task_id="pythonoperator_example",
        python_callable=_pythonoperator_example,
        op_kwargs={
            "launch_cmd": LAUNCH_CMD,
            "check_cmd": CHECK_CMD,
            "out_file": "/tmp/tss_pythonop_{{ run_id }}.out",
        },
        retries=1,
        retry_delay=duration(seconds=5),
    )

    CustomBaseOperator(
        task_id="baseoperator_example",
        launch_cmd=LAUNCH_CMD,
        check_cmd=CHECK_CMD,
        out_file="/tmp/tss_baseop_{{ run_id }}.out",
        retries=1,
        retry_delay=duration(seconds=5),
    )

    CustomSSHOperator(
        task_id="sshoperator_example",
        ssh_conn_id=SSH_CONN_ID,
        launch_cmd=LAUNCH_CMD,
        check_cmd=CHECK_CMD,
        out_file="/tmp/tss_sshop_{{ run_id }}.out",
        cmd_timeout=120,
        retries=1,
        retry_delay=duration(seconds=5),
    )


task_state_store_ssh_variants()