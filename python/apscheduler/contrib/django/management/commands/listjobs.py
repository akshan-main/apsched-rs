"""Django management command to list scheduled jobs."""
try:
    from django.core.management.base import BaseCommand
except ImportError:
    BaseCommand = object


class Command(BaseCommand):
    help = 'List all scheduled jobs'

    def handle(self, *args, **options):
        from apscheduler.contrib.django.scheduler import get_scheduler

        scheduler = get_scheduler()
        jobs = scheduler.get_jobs()

        if not jobs:
            self.stdout.write('No jobs scheduled')
            return

        for job in jobs:
            next_run = getattr(job, 'next_run_time', None)
            self.stdout.write(
                f'  {job.id}: {job.name or "(unnamed)"} -> next run: {next_run}',
            )

        self.stdout.write(f'\nTotal: {len(jobs)} job(s)')
