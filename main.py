from taskq.app import create_app
from taskq.worker import task

app = create_app()


@task("send_email")
def send_email(params: dict):
    to = params.get("to", "unknown")
    subject = params.get("subject", "(no subject)")
    print(f"[send_email] To: {to}, Subject: {subject}")
    return {"sent": True, "to": to}


@task("generate_report")
def generate_report(params: dict):
    report_type = params.get("type", "summary")
    print(f"[generate_report] Generating {report_type} report...")
    return {"report_url": f"/reports/{report_type}.pdf"}


@task("sync_data")
def sync_data(params: dict):
    source = params.get("source", "default")
    print(f"[sync_data] Syncing from {source}")
    return {"synced": True, "source": source}
