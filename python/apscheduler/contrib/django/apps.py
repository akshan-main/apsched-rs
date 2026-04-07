"""Django AppConfig for apscheduler-rs."""
try:
    from django.apps import AppConfig
except ImportError:
    AppConfig = object


class APSchedulerConfig(AppConfig):
    name = 'apscheduler.contrib.django'
    label = 'apscheduler_rs'
    verbose_name = 'APScheduler-rs'

    def ready(self):
        # Triggered when Django finishes loading
        from apscheduler.contrib.django.scheduler import get_scheduler
        # Don't auto-start the scheduler here - that happens in runscheduler command
        # But initialize it so other code can register jobs
        try:
            get_scheduler()
        except Exception:
            pass
