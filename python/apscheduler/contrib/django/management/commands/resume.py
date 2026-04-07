"""Django management command to resume the scheduler or a specific job."""
try:
    from django.core.management.base import BaseCommand
except ImportError:
    BaseCommand = object


class Command(BaseCommand):
    help = 'Resume the scheduler, or a specific job by id'

    def add_arguments(self, parser):
        parser.add_argument(
            '--job-id',
            dest='job_id',
            default=None,
            help='Resume a specific job instead of the whole scheduler',
        )

    def handle(self, *args, **options):
        from apscheduler.contrib.django.scheduler import get_scheduler

        scheduler = get_scheduler()
        job_id = options.get('job_id')

        if job_id:
            scheduler.resume_job(job_id)
            self.stdout.write(self.style.SUCCESS(f'Resumed job: {job_id}'))
        else:
            scheduler.resume()
            self.stdout.write(self.style.SUCCESS('Scheduler resumed'))
