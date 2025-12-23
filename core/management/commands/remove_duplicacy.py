
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from datetime import timedelta
from core.models import ClientProgramEnrollment, Client
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        'Remove duplicate enrollment records for the same client and program '
        'where dates overlap or enrollments are open-ended. '
        'Keeps the best enrollment and removes the rest.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Preview what would be removed without actually removing anything',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output for each removal operation',
        )
        parser.add_argument(
            '--client-id',
            type=int,
            help='Only process enrollments for a specific client ID',
        )
        parser.add_argument(
            '--program-id',
            type=int,
            help='Only process enrollments for a specific program ID',
        )
        parser.add_argument(
            '--stats',
            action='store_true',
            help='Show detailed statistics about duplicates found',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force removal even if dry-run is not run first (use with caution)',
        )

    def ranges_overlap_or_adjacent(self, start1, end1, start2, end2):
        """
        Check if two date ranges overlap or are adjacent (within 1 day).
        
        This handles:
        - Overlapping dates: Jan 1-15 and Jan 10-25 → True
        - Adjacent dates: Jan 1-15 and Jan 16-30 → True (within 1 day)
        - Open-ended enrollments: Jan 1-∞ and Jan 10-25 → True
        - Both open-ended: Jan 1-∞ and Jan 10-∞ → True
        
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
        
        # First range is open-ended (start1 to infinity)
        if end1 is None:
            # Overlaps if: range 2 starts within range 1, OR range 2 ends after range 1 starts
            return start2 >= start1 or (end2 and end2 >= start1)
        
        # Second range is open-ended (start2 to infinity)
        if end2 is None:
            # Overlaps if: range 1 starts within range 2, OR range 1 ends after range 2 starts
            return start1 >= start2 or (end1 and end1 >= start2)
        
        # Both have end dates - check for overlap or adjacency
        # Overlap: start1 <= end2 AND start2 <= end1
        # Adjacent: end1 + 1 day = start2 OR end2 + 1 day = start1
        overlap = start1 <= end2 and start2 <= end1
        adjacent = (end1 and end1 + timedelta(days=1) == start2) or \
                   (end2 and end2 + timedelta(days=1) == start1)
        return overlap or adjacent

    def find_overlapping_groups(self, enrollments):
        """
        Find groups of overlapping enrollments.
        
        Uses the same algorithm as merge_duplicate_enrollments.py to identify
        which enrollments overlap with each other.
        
        Args:
            enrollments: List of ClientProgramEnrollment objects
            
        Returns:
            list: List of groups, where each group contains enrollments that overlap
        """
        if not enrollments:
            return []
        
        # Sort by start_date for easier processing
        sorted_enrollments = sorted(
            enrollments, 
            key=lambda e: (e.start_date or timezone.now().date())
        )
        
        groups = []
        processed = set()
        
        for enrollment in sorted_enrollments:
            if enrollment.id in processed:
                continue
            
            # Start a new group with this enrollment
            group = [enrollment]
            processed.add(enrollment.id)
            
            # Find all enrollments that overlap with any enrollment in this group
            changed = True
            while changed:
                changed = False
                for other in sorted_enrollments:
                    if other.id in processed:
                        continue
                    
                    # Check if this enrollment overlaps with any in the current group
                    for group_member in group:
                        if self.ranges_overlap_or_adjacent(
                            group_member.start_date, group_member.end_date,
                            other.start_date, other.end_date
                        ):
                            group.append(other)
                            processed.add(other.id)
                            changed = True
                            break
            
            # Only return groups with multiple enrollments (duplicates)
            if len(group) > 1:
                groups.append(group)
        
        return groups

    def select_best_enrollment(self, enrollments):
        """
        Select the best enrollment to keep from a group of duplicates.
        
        Selection priority:
        1. Non-archived enrollments (preferred)
        2. Most recent created_at (most up-to-date)
        3. Most complete data (has notes, receiving_services_date)
        4. Earliest start_date (longest period)
        
        Args:
            enrollments: List of ClientProgramEnrollment objects
            
        Returns:
            ClientProgramEnrollment: The enrollment to keep
        """
        if len(enrollments) == 1:
            return enrollments[0]
        
        # Priority 1: Prefer non-archived
        non_archived = [e for e in enrollments if not e.is_archived]
        candidates = non_archived if non_archived else enrollments
        
        # Priority 2: Most recent created_at (most up-to-date)
        candidates.sort(key=lambda e: e.created_at or timezone.now(), reverse=True)
        most_recent = candidates[0]
        
        # Priority 3: Most complete data
        # Score based on: notes, receiving_services_date, status
        def completeness_score(enrollment):
            score = 0
            if enrollment.notes:
                score += 10
            if enrollment.receiving_services_date:
                score += 5
            if enrollment.status and enrollment.status != 'pending':
                score += 3
            if enrollment.sub_program:
                score += 2
            return score
        
        # Among most recent, prefer most complete
        best_score = completeness_score(most_recent)
        for candidate in candidates:
            if candidate.created_at == most_recent.created_at:
                score = completeness_score(candidate)
                if score > best_score:
                    most_recent = candidate
                    best_score = score
        
        # Priority 4: Earliest start_date (longest period)
        same_completeness = [
            e for e in candidates 
            if e.created_at == most_recent.created_at 
            and completeness_score(e) == best_score
        ]
        if same_completeness:
            same_completeness.sort(key=lambda e: e.start_date or timezone.now().date())
            return same_completeness[0]
        
        return most_recent

    def remove_duplicate_group(self, group, dry_run=False, verbose=False):
        """
        Remove duplicate enrollments from a group, keeping only the best one.
        
        Args:
            group: List of overlapping ClientProgramEnrollment objects
            dry_run: If True, only show what would be removed
            verbose: If True, show detailed output
            
        Returns:
            dict: Statistics about the removal operation
        """
        if len(group) <= 1:
            return None
        
        # Select the best enrollment to keep
        enrollment_to_keep = self.select_best_enrollment(group)
        enrollments_to_remove = [e for e in group if e.id != enrollment_to_keep.id]
        
        if verbose:
            client_name = f"{enrollment_to_keep.client.first_name} {enrollment_to_keep.client.last_name}"
            program_name = enrollment_to_keep.program.name
            self.stdout.write(
                f"  Removing {len(enrollments_to_remove)} duplicate enrollment(s) for "
                f"{client_name} in {program_name}"
            )
            self.stdout.write(
                f"    Keeping enrollment ID: {enrollment_to_keep.id} "
                f"({enrollment_to_keep.start_date} to {enrollment_to_keep.end_date or 'ongoing'})"
            )
            self.stdout.write(
                f"    Removing enrollment IDs: {[e.id for e in enrollments_to_remove]}"
            )
            for e in enrollments_to_remove:
                self.stdout.write(
                    f"      - ID {e.id}: {e.start_date} to {e.end_date or 'ongoing'} "
                    f"(created: {e.created_at})"
                )
        
        if not dry_run:
            # Actually remove the duplicate enrollments
            removed_count = 0
            removed_ids = []
            
            for enrollment in enrollments_to_remove:
                try:
                    enrollment_id = enrollment.id
                    enrollment.delete()  # Permanently delete (not archive)
                    removed_count += 1
                    removed_ids.append(enrollment_id)
                    logger.info(
                        f"Removed duplicate enrollment {enrollment_id} for client "
                        f"{enrollment.client_id}, program {enrollment.program_id}"
                    )
                except Exception as e:
                    error_msg = (
                        f"Failed to remove enrollment {enrollment.id}: {str(e)}"
                    )
                    logger.error(error_msg, exc_info=True)
                    if verbose:
                        self.stdout.write(
                            self.style.ERROR(f"    ERROR: {error_msg}")
                        )
            
            # Update client status after removal
            status_updated = False
            try:
                client = Client.objects.get(id=enrollment_to_keep.client_id)
                old_status = client.is_inactive
                status_changed = client.update_inactive_status()
                if status_changed:
                    client.save(update_fields=['is_inactive'])
                    status_updated = True
                    new_status = client.is_inactive
                    status_text = "inactive" if new_status else "active"
                    logger.info(
                        f"Updated client {client.first_name} {client.last_name} "
                        f"(ID: {client.id}) status from "
                        f"{'inactive' if old_status else 'active'} to {status_text} "
                        f"after duplicate removal"
                    )
                    if verbose:
                        self.stdout.write(
                            f"    Client status updated: "
                            f"{'inactive' if old_status else 'active'} → {status_text}"
                        )
            except Exception as e:
                error_msg = (
                    f"Failed to update client status for client "
                    f"{enrollment_to_keep.client_id}: {str(e)}"
                )
                logger.warning(error_msg, exc_info=True)
                if verbose:
                    self.stdout.write(
                        self.style.ERROR(f"    WARNING: {error_msg}")
                    )
            
            return {
                'kept_id': enrollment_to_keep.id,
                'removed_count': removed_count,
                'removed_ids': removed_ids,
                'total_duplicates': len(group),
                'status_updated': status_updated
            }
        else:
            # Dry run - just return what would happen
            return {
                'kept_id': enrollment_to_keep.id,
                'removed_count': len(enrollments_to_remove),
                'removed_ids': [e.id for e in enrollments_to_remove],
                'total_duplicates': len(group),
                'status_updated': False
            }

    def handle(self, *args, **options):
        """
        Main command handler.
        
        Process:
        1. Find all non-archived enrollments
        2. Group by (client, program)
        3. Find overlapping groups within each client-program combination
        4. Remove duplicates, keeping the best enrollment
        5. Update client statuses
        6. Show summary
        """
        dry_run = options['dry_run']
        verbose = options['verbose']
        client_id = options.get('client_id')
        program_id = options.get('program_id')
        stats_mode = options.get('stats', False)
        force = options.get('force', False)
        
        self.stdout.write(
            self.style.SUCCESS('\n=== Duplicate Enrollment Removal Process ===\n')
        )
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING('DRY RUN MODE - No enrollments will be removed\n')
            )
        elif not force:
            self.stdout.write(
                self.style.WARNING(
                    '⚠️  WARNING: This will PERMANENTLY DELETE duplicate enrollments!\n'
                    '⚠️  Run with --dry-run first to preview changes.\n'
                    '⚠️  Use --force to skip this warning.\n'
                )
            )
            return
        
        # Build query for enrollments
        enrollments_query = ClientProgramEnrollment.objects.filter(is_archived=False)
        
        if client_id:
            enrollments_query = enrollments_query.filter(client_id=client_id)
            self.stdout.write(f'Filtering by Client ID: {client_id}')
        
        if program_id:
            enrollments_query = enrollments_query.filter(program_id=program_id)
            self.stdout.write(f'Filtering by Program ID: {program_id}')
        
        # Group enrollments by client and program
        self.stdout.write('Grouping enrollments by client and program...')
        enrollment_groups = defaultdict(list)
        
        for enrollment in enrollments_query.select_related('client', 'program'):
            key = (enrollment.client_id, enrollment.program_id)
            enrollment_groups[key].append(enrollment)
        
        total_groups = len(enrollment_groups)
        total_enrollments = enrollments_query.count()
        self.stdout.write(f'Found {total_enrollments} total enrollments')
        self.stdout.write(f'Found {total_groups} unique client-program combinations\n')
        
        # Statistics tracking
        groups_with_multiple = 0
        groups_with_overlaps = 0
        groups_with_no_overlaps = 0
        total_duplicate_groups = 0
        enrollments_in_groups_with_multiple = 0
        
        # Process each group
        total_removed = 0
        total_kept = 0
        groups_processed = 0
        groups_with_removals = 0
        clients_status_updated = 0
        errors = []
        all_removed_ids = []  # Track all removed enrollment IDs
        removal_details = []  # Track details of what was removed
        
        for (client_id, program_id), enrollments in enrollment_groups.items():
            if len(enrollments) <= 1:
                continue  # No duplicates possible
            
            groups_with_multiple += 1
            enrollments_in_groups_with_multiple += len(enrollments)
            
            # Find overlapping groups within this client-program combination
            overlapping_groups = self.find_overlapping_groups(enrollments)
            
            if not overlapping_groups:
                groups_with_no_overlaps += 1
                if stats_mode:
                    client = enrollments[0].client
                    program = enrollments[0].program
                    # Show why they're not overlapping
                    sorted_dates = sorted([
                        (e.start_date, e.end_date) for e in enrollments
                    ])
                    gaps = []
                    for i in range(len(sorted_dates) - 1):
                        end1 = sorted_dates[i][1]
                        start2 = sorted_dates[i+1][0]
                        if end1 and start2:
                            gap = (start2 - end1).days
                            if gap > 1:  # More than 1 day gap
                                gaps.append(f"{gap} days")
                    if gaps:
                        self.stdout.write(
                            f"  Skipped: {client.first_name} {client.last_name} - "
                            f"{program.name} ({len(enrollments)} enrollments, "
                            f"gaps: {', '.join(gaps)})"
                        )
                continue
            
            groups_with_overlaps += 1
            groups_with_removals += 1
            total_duplicate_groups += len(overlapping_groups)
            
            if verbose:
                client = enrollments[0].client
                program = enrollments[0].program
                self.stdout.write(
                    f"\nProcessing: {client.first_name} {client.last_name} - "
                    f"{program.name}"
                )
            
            for group in overlapping_groups:
                try:
                    # Each removal is in its own transaction
                    with transaction.atomic():
                        result = self.remove_duplicate_group(
                            group, dry_run=dry_run, verbose=verbose
                        )
                        if result:
                            total_removed += result['removed_count']
                            total_kept += 1  # One enrollment kept per group
                            # Track removed IDs and details
                            if result.get('removed_ids'):
                                all_removed_ids.extend(result['removed_ids'])
                                # Store details for summary
                                client = enrollments[0].client
                                program = enrollments[0].program
                                removal_details.append({
                                    'client_id': client.id,
                                    'client_name': f"{client.first_name} {client.last_name}",
                                    'program_id': program.id,
                                    'program_name': program.name,
                                    'kept_id': result.get('kept_id'),
                                    'removed_ids': result.get('removed_ids', []),
                                    'removed_count': result.get('removed_count', 0)
                                })
                            # Track if client status was updated
                            if result.get('status_updated'):
                                clients_status_updated += 1
                except Exception as e:
                    error_msg = (
                        f"Error removing duplicates for client {enrollments[0].client.id}, "
                        f"program {enrollments[0].program.id}: {str(e)}"
                    )
                    errors.append(error_msg)
                    logger.error(error_msg, exc_info=True)
                    if verbose:
                        self.stdout.write(
                            self.style.ERROR(f"  ERROR: {error_msg}")
                        )
                    # Continue processing other groups even if one fails
        
        # Final pass: Update status for all clients that had enrollments processed
        if not dry_run:
            self.stdout.write(
                '\nUpdating client statuses for all affected clients...'
            )
            affected_client_ids = set()
            for (client_id, program_id), enrollments in enrollment_groups.items():
                if len(enrollments) > 1:  # Only clients with potential duplicates
                    affected_client_ids.add(client_id)
            
            status_updates_final = 0
            for client_id in affected_client_ids:
                try:
                    client = Client.objects.get(id=client_id)
                    old_status = client.is_inactive
                    status_changed = client.update_inactive_status()
                    if status_changed:
                        client.save(update_fields=['is_inactive'])
                        status_updates_final += 1
                        if verbose:
                            new_status = "inactive" if client.is_inactive else "active"
                            self.stdout.write(
                                f"  Updated client {client.first_name} "
                                f"{client.last_name} (ID: {client_id}) to {new_status}"
                            )
                except Exception as e:
                    logger.warning(
                        f"Failed to update client status for client {client_id}: {e}"
                    )
            
            if status_updates_final > 0:
                self.stdout.write(
                    f'Updated status for {status_updates_final} additional client(s)'
                )
        
        # Summary
        self.stdout.write(self.style.SUCCESS('\n=== Summary ===\n'))
        self.stdout.write(f'Total enrollments processed: {total_enrollments}')
        self.stdout.write(f'Unique client-program combinations: {total_groups}')
        self.stdout.write(
            f'Client-program combinations with 2+ enrollments: {groups_with_multiple}'
        )
        self.stdout.write(f'  - With overlapping dates: {groups_with_overlaps}')
        self.stdout.write(
            f'  - Without overlapping dates (gaps > 1 day): {groups_with_no_overlaps}'
        )
        self.stdout.write(f'Groups with duplicates removed: {groups_with_removals}')
        self.stdout.write(f'Total duplicate enrollments removed: {total_removed}')
        self.stdout.write(f'Total enrollments kept: {total_kept}')
        self.stdout.write(f'Clients with status updated: {clients_status_updated}')
        
        if stats_mode:
            self.stdout.write(
                self.style.SUCCESS('\n=== Detailed Statistics ===\n')
            )
            self.stdout.write(
                f'Enrollments in groups with multiple: {enrollments_in_groups_with_multiple}'
            )
            self.stdout.write(
                f'Enrollments in single-enrollment groups: '
                f'{total_enrollments - enrollments_in_groups_with_multiple}'
            )
            if groups_with_multiple > 0:
                avg = enrollments_in_groups_with_multiple / groups_with_multiple
                self.stdout.write(
                    f'Average enrollments per client-program (with multiples): {avg:.2f}'
                )
            if groups_with_no_overlaps > 0:
                self.stdout.write(
                    self.style.WARNING(
                        f'\n⚠️  Note: {groups_with_no_overlaps} client-program combinations '
                        f'have multiple enrollments but they don\'t overlap (gaps > 1 day). '
                        f'These are NOT removed as they may represent separate enrollment periods.'
                    )
                )
        
        # Display detailed removal information
        if all_removed_ids and not dry_run:
            self.stdout.write(
                self.style.SUCCESS('\n=== Removed Enrollment Details ===\n')
            )
            self.stdout.write(
                f'Total enrollment IDs removed: {len(all_removed_ids)}\n'
            )
            self.stdout.write('Removed Enrollment IDs: ' + ', '.join(map(str, all_removed_ids)))
            self.stdout.write('\n\nDetailed breakdown:\n')
            for detail in removal_details:
                self.stdout.write(
                    f"  Client: {detail['client_name']} (ID: {detail['client_id']}) | "
                    f"Program: {detail['program_name']} (ID: {detail['program_id']})\n"
                    f"    Kept enrollment ID: {detail['kept_id']}\n"
                    f"    Removed enrollment IDs: {', '.join(map(str, detail['removed_ids']))}\n"
                )
            self.stdout.write(
                '\n💡 Note: These records have been PERMANENTLY DELETED from the database.\n'
                '💡 If you don\'t see them in the UI, check your filters (date range, department, program, etc.)\n'
            )
        
        if errors:
            self.stdout.write(
                self.style.ERROR(f'\n⚠️  Errors encountered: {len(errors)}')
            )
            if verbose:
                for error in errors:
                    self.stdout.write(self.style.ERROR(f'  - {error}'))
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING('\nDRY RUN - No enrollments were removed')
            )
        else:
            if errors:
                self.stdout.write(
                    self.style.WARNING(
                        '\n⚠️  Removal process completed with errors'
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        '\n✅ Duplicate removal process completed successfully!'
                    )
                )
        
        self.stdout.write('')

