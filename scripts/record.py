import sentry_sdk
from sentry_sdk.crons import capture_checkin
from sentry_sdk.crons.consts import MonitorStatus

from envoy_recorder.envoy_recorder import EnvoyRecorder
from envoy_recorder.logging import get_logger

log = get_logger(__name__)

monitor_slug = "envoy_reader"


def start_sentry() -> str:
    sentry_sdk.init(
        dsn="https://fed4b02053480412686e0cdb49e8c7bd@o4510693003034624.ingest.de.sentry.io/4510693008146512",
        # Add data like request headers and IP for users,
        # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
        send_default_pii=True,
        enable_logs=True,
    )

    # All keys except 'schedule' are optional
    monitor_config = {
        "schedule": {"type": "crontab", "value": "* * * * *"},
        "timezone": "Europe/London",
        # If an expected check-in doesn't come in 'checkin_margin' minutes, it'll be considered missed
        "checkin_margin": 5,
        # The check-in is allowed to run for 'max_runtime' minutes before it's considered failed
        "max_runtime": 10,
        # It'll take 'failure_issue_threshold' consecutive failed check-ins to create an issue
        "failure_issue_threshold": 5,
        # It'll take 'recovery_threshold' OK check-ins to resolve an issue
        "recovery_threshold": 5,
    }

    check_in_id = capture_checkin(
        monitor_config=monitor_config,
        monitor_slug=monitor_slug,
        status=MonitorStatus.IN_PROGRESS,
    )
    return check_in_id


def stop_sentry(check_in_id: str, status: MonitorStatus):
    capture_checkin(
        monitor_slug=monitor_slug,
        check_in_id=check_in_id,
        status=status,
    )


def main():
    check_in_id = start_sentry()
    log.info("Starting up!")
    try:
        envoy_recorder = EnvoyRecorder()
        envoy_recorder.run()
    except:
        log.exception("Exception raised in main()!")
        stop_sentry(check_in_id, status=MonitorStatus.ERROR)
        raise
    else:
        stop_sentry(check_in_id, status=MonitorStatus.OK)
        log.info("Finished successfully!\n")


if __name__ == "__main__":
    main()
