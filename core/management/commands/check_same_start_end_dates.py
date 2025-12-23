from django.core.management.base import BaseCommand
from django.db import models
from core.models import ClientProgramEnrollment
import logging
import csv
import os

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Check for enrollments where start_date and end_date are the same'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output',
        )
        parser.add_argument(
            '--client-id',
            type=int,
            help='Only check enrollments for a specific client ID',
        )
        parser.add_argument(
            '--program-id',
            type=int,
            help='Only check enrollments for a specific program ID',
        )
        parser.add_argument(
            '--export-csv',
            type=str,
            help='Export results to CSV file (provide filename)',
        )

    def handle(self, *args, **options):
        verbose = options.get('verbose', False)
        client_id = options.get('client_id')
        program_id = options.get('program_id')
        export_csv = options.get('export_csv')

        self.stdout.write('Checking for enrollments with same start and end dates...\n')

        query = ClientProgramEnrollment.objects.filter(
            start_date__isnull=False,
            end_date__isnull=False
        ).filter(start_date=models.F('end_date'))

        if client_id:
            query = query.filter(client_id=client_id)
            self.stdout.write(f'Filtering by Client ID: {client_id}')

        if program_id:
            query = query.filter(program_id=program_id)
            self.stdout.write(f'Filtering by Program ID: {program_id}')

        enrollments = query.select_related('client', 'program')

        count = enrollments.count()
        self.stdout.write(f'Found {count} enrollment(s) with same start and end dates\n')

        if count > 0:
            for enrollment in enrollments:
                client_name = f"{enrollment.client.first_name} {enrollment.client.last_name}"
                program_name = enrollment.program.name
                message = (
                    f"Enrollment ID {enrollment.id}: Client '{client_name}' (ID: {enrollment.client_id}) "
                    f"in Program '{program_name}' (ID: {enrollment.program_id}) "
                    f"has same start and end date: {enrollment.start_date}"
                )
                
                logger.warning(message)
                
                if verbose:
                    self.stdout.write(
                        f"  - {message}\n"
                        f"    Status: {enrollment.status}, "
                        f"Created: {enrollment.created_at}, "
                        f"Archived: {enrollment.is_archived}"
                    )
            
            # Export to CSV if requested
            if export_csv:
                self.export_to_csv(enrollments, export_csv)
                self.stdout.write(self.style.SUCCESS(f'\n✅ Results exported to: {export_csv}'))
        else:
            self.stdout.write(self.style.SUCCESS('No enrollments found with same start and end dates'))

        self.stdout.write('')

    def export_to_csv(self, enrollments, filename):
        """Export enrollment results to CSV file."""
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'enrollment_id',
                'client_id',
                'client_name',
                'client_email',
                'program_id',
                'program_name',
                'start_date',
                'end_date',
                'status',
                'is_archived',
                'created_at',
                'updated_at',
                'notes',
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for enrollment in enrollments:
                client_name = f"{enrollment.client.first_name} {enrollment.client.last_name}"
                writer.writerow({
                    'enrollment_id': enrollment.id,
                    'client_id': enrollment.client_id,
                    'client_name': client_name,
                    'client_email': enrollment.client.email or '',
                    'program_id': enrollment.program_id,
                    'program_name': enrollment.program.name,
                    'start_date': enrollment.start_date,
                    'end_date': enrollment.end_date,
                    'status': enrollment.status,
                    'is_archived': enrollment.is_archived,
                    'created_at': enrollment.created_at.strftime('%Y-%m-%d %H:%M:%S') if enrollment.created_at else '',
                    'updated_at': enrollment.updated_at.strftime('%Y-%m-%d %H:%M:%S') if enrollment.updated_at else '',
                    'notes': enrollment.notes or '',
                })

