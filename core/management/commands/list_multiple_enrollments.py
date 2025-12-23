"""
Django management command to list all clients with multiple enrollments for the same program.

This command identifies and displays clients who have duplicate or multiple enrollment
records for the same program, helping you identify potential duplicates before running
merge_duplicate_enrollments.py or remove_duplicacy.py.

WHAT IT SHOWS:
-------------
- Clients with 2+ enrollments in the same program
- Enrollment details (dates, status, IDs)
- Whether enrollments overlap or have gaps
- Statistics about duplicate patterns

USE CASES:
---------
1. Identify potential duplicates before cleanup
2. Audit data quality issues
3. Find clients with overlapping enrollments
4. Generate reports on enrollment patterns

Usage:
------
# List all clients with multiple enrollments
python manage.py list_multiple_enrollments

# Show detailed information
python manage.py list_multiple_enrollments --verbose

# Filter by specific client
python manage.py list_multiple_enrollments --client-id 123

# Filter by specific program
python manage.py list_multiple_enrollments --program-id 45

# Show only overlapping enrollments
python manage.py list_multiple_enrollments --overlapping-only

# Export to CSV
python manage.py list_multiple_enrollments --export-csv output.csv

# Show statistics summary
python manage.py list_multiple_enrollments --stats
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from core.models import ClientProgramEnrollment, Client, Program
from collections import defaultdict
import csv
import os


class Command(BaseCommand):
    help = (
        'List all clients with multiple enrollments for the same program. '
        'Shows enrollment details and identifies overlapping vs non-overlapping enrollments.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed information for each enrollment',
        )
        parser.add_argument(
            '--client-id',
            type=int,
            help='Only show enrollments for a specific client ID',
        )
        parser.add_argument(
            '--program-id',
            type=int,
            help='Only show enrollments for a specific program ID',
        )
        parser.add_argument(
            '--overlapping-only',
            action='store_true',
            help='Only show clients with overlapping enrollments (exclude gaps > 1 day)',
        )
        parser.add_argument(
            '--stats',
            action='store_true',
            help='Show summary statistics',
        )
        parser.add_argument(
            '--export-csv',
            type=str,
            help='Export results to CSV file (provide filename)',
        )
        parser.add_argument(
            '--include-archived',
            action='store_true',
            help='Include archived enrollments in the analysis',
        )

    def ranges_overlap_or_adjacent(self, start1, end1, start2, end2):
        """
        Check if two date ranges overlap or are adjacent (within 1 day).
        
        Args:
            start1: Start date of first range
            end1: End date of first range (None if open-ended)
            start2: Start date of second range
            end2: End date of second range (None if open-ended)
            
        Returns:
            bool: True if ranges overlap or are adjacent
        """
        # Both open-ended - they overlap
        if end1 is None and end2 is None:
            return True
        
        # First range is open-ended
        if end1 is None:
            return start2 >= start1 or (end2 and end2 >= start1)
        
        # Second range is open-ended
        if end2 is None:
            return start1 >= start2 or (end1 and end1 >= start2)
        
        # Both have end dates - check for overlap or adjacency
        overlap = start1 <= end2 and start2 <= end1
        adjacent = (end1 and end1 + timedelta(days=1) == start2) or \
                   (end2 and end2 + timedelta(days=1) == start1)
        return overlap or adjacent

    def check_enrollments_overlap(self, enrollments):
        """
        Check if any enrollments in a group overlap.
        
        Args:
            enrollments: List of ClientProgramEnrollment objects
            
        Returns:
            tuple: (has_overlap: bool, overlapping_pairs: list)
        """
        if len(enrollments) < 2:
            return False, []
        
        overlapping_pairs = []
        for i, e1 in enumerate(enrollments):
            for e2 in enrollments[i+1:]:
                if self.ranges_overlap_or_adjacent(
                    e1.start_date, e1.end_date,
                    e2.start_date, e2.end_date
                ):
                    overlapping_pairs.append((e1, e2))
        
        return len(overlapping_pairs) > 0, overlapping_pairs

    def format_date_range(self, start_date, end_date):
        """Format date range for display."""
        if end_date:
            return f"{start_date} to {end_date}"
        else:
            return f"{start_date} to (ongoing)"

    def format_enrollment_info(self, enrollment, verbose=False):
        """Format enrollment information for display."""
        date_range = self.format_date_range(enrollment.start_date, enrollment.end_date)
        info = f"ID: {enrollment.id} | {date_range} | Status: {enrollment.status}"
        
        if verbose:
            info += f" | Created: {enrollment.created_at.strftime('%Y-%m-%d %H:%M')}"
            if enrollment.notes:
                notes_preview = enrollment.notes[:50] + "..." if len(enrollment.notes) > 50 else enrollment.notes
                info += f" | Notes: {notes_preview}"
            if enrollment.receiving_services_date:
                info += f" | Services Date: {enrollment.receiving_services_date}"
            if enrollment.is_archived:
                info += " | [ARCHIVED]"
        
        return info

    def handle(self, *args, **options):
        """
        Main command handler.
        
        Process:
        1. Query enrollments (filter by client/program if specified)
        2. Group by (client, program)
        3. Filter to groups with 2+ enrollments
        4. Check for overlaps
        5. Display results
        6. Export to CSV if requested
        """
        verbose = options['verbose']
        client_id = options.get('client_id')
        program_id = options.get('program_id')
        overlapping_only = options.get('overlapping_only', False)
        stats_mode = options.get('stats', False)
        export_csv = options.get('export_csv')
        include_archived = options.get('include_archived', False)
        
        self.stdout.write(
            self.style.SUCCESS('\n=== Clients with Multiple Enrollments ===\n')
        )
        
        # Build query
        enrollments_query = ClientProgramEnrollment.objects.all()
        
        if not include_archived:
            enrollments_query = enrollments_query.filter(is_archived=False)
        
        if client_id:
            enrollments_query = enrollments_query.filter(client_id=client_id)
            self.stdout.write(f'Filtering by Client ID: {client_id}')
        
        if program_id:
            enrollments_query = enrollments_query.filter(program_id=program_id)
            self.stdout.write(f'Filtering by Program ID: {program_id}')
        
        if include_archived:
            self.stdout.write('Including archived enrollments')
        
        # Group enrollments by client and program
        self.stdout.write('Analyzing enrollments...')
        enrollment_groups = defaultdict(list)
        
        for enrollment in enrollments_query.select_related('client', 'program'):
            key = (enrollment.client_id, enrollment.program_id)
            enrollment_groups[key].append(enrollment)
        
        # Filter to groups with multiple enrollments
        multiple_enrollment_groups = {
            k: v for k, v in enrollment_groups.items() 
            if len(v) > 1
        }
        
        total_enrollments = enrollments_query.count()
        total_groups = len(enrollment_groups)
        groups_with_multiple = len(multiple_enrollment_groups)
        
        self.stdout.write(f'Total enrollments analyzed: {total_enrollments}')
        self.stdout.write(f'Total client-program combinations: {total_groups}')
        self.stdout.write(
            f'Client-program combinations with 2+ enrollments: {groups_with_multiple}\n'
        )
        
        if groups_with_multiple == 0:
            self.stdout.write(
                self.style.SUCCESS('✅ No clients found with multiple enrollments for the same program!')
            )
            self.stdout.write('')
            return
        
        # Process each group
        results = []
        groups_with_overlaps = 0
        groups_without_overlaps = 0
        total_duplicate_enrollments = 0
        
        for (client_id, program_id), enrollments in multiple_enrollment_groups.items():
            # Sort enrollments by start_date
            enrollments.sort(key=lambda e: e.start_date or timezone.now().date())
            
            # Check for overlaps
            has_overlap, overlapping_pairs = self.check_enrollments_overlap(enrollments)
            
            # Skip if only showing overlapping and this group doesn't overlap
            if overlapping_only and not has_overlap:
                continue
            
            if has_overlap:
                groups_with_overlaps += 1
            else:
                groups_without_overlaps += 1
            
            total_duplicate_enrollments += len(enrollments)
            
            client = enrollments[0].client
            program = enrollments[0].program
            
            # Store result for CSV export
            result_entry = {
                'client_id': client.id,
                'client_name': f"{client.first_name} {client.last_name}",
                'client_email': client.email or '',
                'program_id': program.id,
                'program_name': program.name,
                'enrollment_count': len(enrollments),
                'has_overlap': has_overlap,
                'overlapping_pairs_count': len(overlapping_pairs),
                'enrollments': enrollments
            }
            results.append(result_entry)
            
            # Display
            self.stdout.write(
                self.style.WARNING(
                    f"\n{'='*80}\n"
                    f"Client: {client.first_name} {client.last_name} (ID: {client.id})\n"
                    f"Program: {program.name} (ID: {program.id})\n"
                    f"Enrollments: {len(enrollments)}"
                )
            )
            
            if has_overlap:
                self.stdout.write(
                    self.style.ERROR(f"⚠️  OVERLAPPING: {len(overlapping_pairs)} overlapping pair(s)")
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS("✓ Non-overlapping (separate periods)")
                )
            
            self.stdout.write("\nEnrollment Details:")
            for i, enrollment in enumerate(enrollments, 1):
                date_range = self.format_date_range(
                    enrollment.start_date, enrollment.end_date
                )
                self.stdout.write(
                    f"  {i}. {self.format_enrollment_info(enrollment, verbose=verbose)}"
                )
            
            if verbose and overlapping_pairs:
                self.stdout.write("\nOverlapping Pairs:")
                for e1, e2 in overlapping_pairs:
                    self.stdout.write(
                        f"  - Enrollment {e1.id} overlaps with Enrollment {e2.id}"
                    )
                    self.stdout.write(
                        f"    {self.format_date_range(e1.start_date, e1.end_date)} "
                        f"overlaps with "
                        f"{self.format_date_range(e2.start_date, e2.end_date)}"
                    )
        
        # Summary
        displayed_groups = len(results)
        self.stdout.write(self.style.SUCCESS(f'\n{"="*80}\n=== Summary ===\n'))
        self.stdout.write(f'Total client-program combinations analyzed: {total_groups}')
        self.stdout.write(
            f'Combinations with 2+ enrollments: {groups_with_multiple}'
        )
        self.stdout.write(f'  - With overlapping dates: {groups_with_overlaps}')
        self.stdout.write(
            f'  - Without overlapping dates (gaps > 1 day): {groups_without_overlaps}'
        )
        self.stdout.write(f'Total duplicate enrollments: {total_duplicate_enrollments}')
        self.stdout.write(f'Groups displayed: {displayed_groups}')
        
        if stats_mode:
            self.stdout.write(self.style.SUCCESS('\n=== Detailed Statistics ===\n'))
            if groups_with_multiple > 0:
                avg_enrollments = total_duplicate_enrollments / groups_with_multiple
                self.stdout.write(
                    f'Average enrollments per duplicate group: {avg_enrollments:.2f}'
                )
            
            # Count by enrollment count
            enrollment_count_distribution = defaultdict(int)
            for result in results:
                enrollment_count_distribution[result['enrollment_count']] += 1
            
            self.stdout.write('\nDistribution by enrollment count:')
            for count in sorted(enrollment_count_distribution.keys()):
                groups = enrollment_count_distribution[count]
                self.stdout.write(f'  {count} enrollments: {groups} client-program combination(s)')
        
        # Export to CSV
        if export_csv:
            self.export_to_csv(results, export_csv, verbose)
            self.stdout.write(
                self.style.SUCCESS(f'\n✅ Results exported to: {export_csv}')
            )
        
        # Recommendations
        if groups_with_overlaps > 0:
            self.stdout.write(
                self.style.WARNING(
                    f'\n💡 Recommendation: {groups_with_overlaps} client-program combination(s) '
                    f'have overlapping enrollments.\n'
                    f'   Consider running:\n'
                    f'   - python manage.py merge_duplicate_enrollments --dry-run\n'
                    f'   - python manage.py remove_duplicacy --dry-run\n'
                )
            )
        
        self.stdout.write('')

    def export_to_csv(self, results, filename, verbose):
        """
        Export results to CSV file.
        
        Args:
            results: List of result dictionaries
            filename: Output CSV filename
            verbose: Include detailed information
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'client_id',
                'client_name',
                'client_email',
                'program_id',
                'program_name',
                'enrollment_count',
                'has_overlap',
                'overlapping_pairs_count',
                'enrollment_ids',
                'enrollment_dates',
                'enrollment_statuses',
            ]
            
            if verbose:
                fieldnames.extend([
                    'enrollment_created_dates',
                    'enrollment_notes',
                ])
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                enrollments = result['enrollments']
                enrollment_ids = [str(e.id) for e in enrollments]
                enrollment_dates = [
                    self.format_date_range(e.start_date, e.end_date) 
                    for e in enrollments
                ]
                enrollment_statuses = [e.status for e in enrollments]
                
                row = {
                    'client_id': result['client_id'],
                    'client_name': result['client_name'],
                    'client_email': result['client_email'],
                    'program_id': result['program_id'],
                    'program_name': result['program_name'],
                    'enrollment_count': result['enrollment_count'],
                    'has_overlap': 'Yes' if result['has_overlap'] else 'No',
                    'overlapping_pairs_count': result['overlapping_pairs_count'],
                    'enrollment_ids': '; '.join(enrollment_ids),
                    'enrollment_dates': '; '.join(enrollment_dates),
                    'enrollment_statuses': '; '.join(enrollment_statuses),
                }
                
                if verbose:
                    enrollment_created = [
                        e.created_at.strftime('%Y-%m-%d %H:%M') if e.created_at else ''
                        for e in enrollments
                    ]
                    enrollment_notes = [
                        (e.notes[:100] if e.notes else '') for e in enrollments
                    ]
                    row['enrollment_created_dates'] = '; '.join(enrollment_created)
                    row['enrollment_notes'] = '; '.join(enrollment_notes)
                
                writer.writerow(row)

