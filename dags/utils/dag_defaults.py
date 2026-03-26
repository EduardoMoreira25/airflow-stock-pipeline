# dags/utils/dag_defaults.py

import logging
from datetime import timedelta

logger = logging.getLogger(__name__)


def on_failure_callback(context):
    """
    Called automatically when a task fails all retries.
    Logs a structured failure report — in production you'd
    send this to Slack or PagerDuty.
    """
    from airflow.models import Variable # type: ignore

    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    logical_date = context["logical_date"]
    run_id = context["run_id"]
    exception = context.get("exception")
    try_number = context["ti"].try_number

    message = (
        f"\n{'='*60}\n"
        f"PIPELINE FAILURE ALERT\n"
        f"{'='*60}\n"
        f"DAG:          {dag_id}\n"
        f"Task:         {task_id}\n"
        f"Logical Date: {logical_date.date()}\n"
        f"Run ID:       {run_id}\n"
        f"Try Number:   {try_number}\n"
        f"Exception:    {exception}\n"
        f"{'='*60}"
    )

    logger.error(message)

    # Send email alert using Airflow's built-in email utility
    try:
        from airflow.utils.email import send_email # type: ignore
        alert_email = Variable.get("ALERT_EMAIL", default_var=None)

        if alert_email:
            send_email(
                to=alert_email,
                subject=f"[Airflow] FAILED: {dag_id}.{task_id} — {logical_date.date()}",
                html_content=f"<pre>{message}</pre>",
            )
            logger.info(f"Alert email sent to {alert_email}")
        else:
            logger.warning("ALERT_EMAIL variable not set — skipping email alert")

    except Exception as e:
        logger.error(f"Failed to send alert email: {e}")
        # Never let the callback itself crash — that causes confusing errors


def on_retry_callback(context):
    """Called on each retry attempt — useful for logging retry patterns."""
    task_id = context["task"].task_id
    try_number = context["ti"].try_number
    exception = context.get("exception")
    logger.warning(
        f"RETRYING task '{task_id}' "
        f"(attempt {try_number}) — {exception}"
    )


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.warning(
        f"SLA MISSED on {dag.dag_id}: "
        f"tasks {task_list} exceeded their SLA"
    )


default_args = {
    "owner": "miendes",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": False,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,   # We use our own callback instead
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback, # Specified here
    "on_retry_callback": on_retry_callback, # And here
}

daily_args = {
    **default_args,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "retry_delay": timedelta(minutes=15),
}