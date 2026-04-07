"""Django management command to run the apscheduler-rs scheduler."""
import logging
import signal
import sys
import time

try:
    from django.core.management.base import BaseCommand
except ImportError:
    BaseCommand = object

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run the apscheduler-rs scheduler'

    def handle(self, *args, **options):
        from apscheduler.contrib.django.scheduler import get_scheduler

        scheduler = get_scheduler()
        scheduler.start()

        self.stdout.write(self.style.SUCCESS('apscheduler-rs scheduler started'))
        self.stdout.write(f'Jobs: {len(scheduler.get_jobs())}')

        def shutdown_handler(signum, frame):
            self.stdout.write('Shutting down scheduler...')
            scheduler.shutdown(wait=True)
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            shutdown_handler(None, None)
