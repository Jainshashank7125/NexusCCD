from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView, View
from django.urls import reverse_lazy, reverse
from django.shortcuts import redirect
from django.utils import timezone
from django.db import models
from django.db.models import Q, Exists, OuterRef
from django.http import HttpResponse
from core.models import Program, Department, ClientProgramEnrollment, ProgramManagerAssignment, Staff
from core.views import jwt_required, ProgramManagerAccessMixin, AnalystAccessMixin, StaffAccessControlMixin, can_see_archived
from core.message_utils import success_message, error_message, warning_message, info_message, create_success, update_success, delete_success, validation_error, permission_error, not_found_error
from django.utils.decorators import method_decorator
import csv
import io
from datetime import timedelta

from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
import json

@method_decorator(jwt_required, name='dispatch')
class ProgramListView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, ListView):
    model = Program
    template_name = 'programs/program_list.html'
    context_object_name = 'programs'
    paginate_by = 10
    
    def get_paginate_by(self, queryset):
        """Get the number of items to paginate by from request parameters"""
        per_page = self.request.GET.get('per_page', '10')
        try:
            per_page = int(per_page)
            # Limit to reasonable values
            if per_page < 5:
                per_page = 5
            elif per_page > 100:
                per_page = 100
        except (ValueError, TypeError):
            per_page = 10
        return per_page

    def get_queryset(self):
        # First apply the ProgramManagerAccessMixin filtering
        queryset = super().get_queryset()
        # Always exclude archived programs - archived programs should not be shown in the programs list
        queryset = queryset.filter(is_archived=False)
        # Exclude programs with archived departments for non-admin users
        if not can_see_archived(self.request.user):
            queryset = queryset.filter(department__is_archived=False)
        # Exclude programs assigned to HASS department (deleted department)
        queryset = queryset.exclude(department__name__iexact='HASS')
        
        # For staff-only users, filter to only their assigned programs
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    # Staff-only users see ONLY programs where their assigned clients are enrolled
                    from staff.models import StaffClientAssignment
                    
                    # Get programs where their assigned clients are enrolled
                    assigned_client_ids = StaffClientAssignment.objects.filter(
                        staff=staff,
                        is_active=True
                    ).values_list('client_id', flat=True)
                    
                    if assigned_client_ids:
                        queryset = queryset.filter(
                            clientprogramenrollment__client_id__in=assigned_client_ids
                        ).distinct()
                    else:
                        # If no client assignments, show no programs
                        queryset = queryset.none()
            except Exception:
                pass
        
        # Apply additional filters
        department_filter = self.request.GET.get('department', '')
        status_filter = self.request.GET.get('status', '')
        program_filter = self.request.GET.get('program', '').strip()
        capacity_filter = self.request.GET.get('capacity', '')
        search_query = self.request.GET.get('search', '').strip()
        
        if department_filter:
            queryset = queryset.filter(department__name=department_filter)
        
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        # Apply program filter before capacity filter (which may convert to list)
        if program_filter:
            try:
                program_id = int(program_filter)
                queryset = queryset.filter(id=program_id)
            except (ValueError, TypeError):
                pass
        
        if capacity_filter:
            if capacity_filter == 'at_capacity':
                # Filter programs that are at or over capacity
                queryset = [p for p in queryset if p.is_at_capacity()]
            elif capacity_filter == 'available':
                # Filter programs with available capacity (including programs with no capacity limit)
                queryset = [
                    p for p in queryset
                    if not p.is_at_capacity() and (p.get_available_capacity() is None or p.get_available_capacity() > 0)
                ]
            elif capacity_filter == 'no_limit':
                # Filter programs with no capacity limit
                if hasattr(queryset, 'filter'):
                    queryset = queryset.filter(Q(no_capacity_limit=True) | Q(capacity_current__lte=0))
                else:
                    queryset = [p for p in queryset if getattr(p, 'no_capacity_limit', False) or p.capacity_current <= 0]
        
        if search_query:
            from django.db.models import Q
            queryset = queryset.filter(
                Q(name__icontains=search_query) |
                Q(department__name__icontains=search_query) |
                Q(location__icontains=search_query) |
                Q(description__icontains=search_query)
            ).distinct()
        
        # Sorting (case-insensitive by relevant text fields)
        from django.db.models.functions import Lower, Coalesce
        from django.db.models import Value
        try:
            queryset = queryset.annotate(
                name_ci=Lower(Coalesce('name', Value(''))),
                department_name_ci=Lower(Coalesce(models.F('department__name'), Value(''))),
                location_ci=Lower(Coalesce('location', Value(''))),
            )
        except Exception:
            # If queryset was converted to a list due to capacity filtering, skip annotations
            pass
        sort_key = self.request.GET.get('sort', 'name_asc')
        sort_mapping = {
            'name_asc': ['name_ci'],
            'name_desc': ['-name_ci'],
            'department_asc': ['department_name_ci', 'name_ci'],
            'department_desc': ['-department_name_ci', 'name_ci'],
            'status_asc': ['status', 'name'],
            'status_desc': ['-status', 'name'],
            'created_desc': ['-created_at'],
            'created_asc': ['created_at'],
            'updated_desc': ['-updated_at'],
            'updated_asc': ['updated_at'],
            'location_asc': ['location_ci', 'name_ci'],
            'location_desc': ['-location_ci', 'name_ci'],
        }
        order_by_fields = sort_mapping.get(sort_key, ['name_ci'])
        try:
            return queryset.order_by(*order_by_fields)
        except Exception:
            # In case queryset was converted to list by capacity filter above
            return queryset
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        programs = context['programs']
        time_filter = self.request.GET.get('time_filter', '')
        
        # Create program data with capacity information
        programs_with_capacity = []
        for program in programs:
            # Use total enrollments (including future) for display
            total_enrollments = program.get_total_enrollments_count()
            current_enrollments = program.get_current_enrollments_count()
            capacity_percentage = program.get_capacity_percentage()
            available_capacity = program.get_available_capacity()
            is_at_capacity = program.is_at_capacity()
            if program.no_capacity_limit or program.capacity_current <= 0:
                display_bar_percentage = 100 if total_enrollments > 0 else 0
            else:
                display_bar_percentage = capacity_percentage
            
            programs_with_capacity.append({
                'program': program,
                'current_enrollments': total_enrollments,  # Show total enrollments including future
                'capacity_percentage': capacity_percentage,
                'available_capacity': available_capacity,
                'is_at_capacity': is_at_capacity,
                'no_capacity_limit': program.no_capacity_limit or program.capacity_current <= 0,
                'display_bar_percentage': display_bar_percentage,
            })
        
        # Get the total count of filtered programs (not just current page)
        queryset = self.get_queryset()
        from django.db.models.query import QuerySet
        if isinstance(queryset, QuerySet):
            # It's a QuerySet
            total_filtered_count = queryset.count()
        else:
            # It's a list (from capacity filtering)
            total_filtered_count = len(queryset)
        
        # Calculate status card counts (assigned/unassigned/total)
        # Use base queryset with permission filters but without search/filter params
        today = timezone.now().date()
        # Always exclude archived programs from summary counts
        base_queryset = Program.objects.filter(is_archived=False)
        # Exclude programs with archived departments for non-admin users
        if not can_see_archived(self.request.user):
            base_queryset = base_queryset.filter(department__is_archived=False)
        base_queryset = base_queryset.exclude(
            department__name__iexact='HASS'
        )
        
        # Apply the same permission filters as ProgramManagerAccessMixin and get_queryset
        if not self.request.user.is_authenticated:
            base_queryset = base_queryset.none()
        elif not self.request.user.is_superuser:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Manager: sees only their assigned programs
                if staff.is_program_manager():
                    assigned_programs = staff.get_assigned_programs()
                    base_queryset = base_queryset.filter(id__in=assigned_programs)
                
                # Leader: sees programs in their assigned departments
                elif staff.is_leader():
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True,
                        is_archived=False
                    ).distinct()
                    base_queryset = base_queryset.filter(department__in=assigned_departments)
                
                # Staff-only users: see ONLY programs where their assigned clients are enrolled
                elif 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    from staff.models import StaffClientAssignment
                    assigned_client_ids = StaffClientAssignment.objects.filter(
                        staff=staff,
                        is_active=True
                    ).values_list('client_id', flat=True)
                    
                    if assigned_client_ids:
                        base_queryset = base_queryset.filter(
                            clientprogramenrollment__client_id__in=assigned_client_ids
                        ).distinct()
                    else:
                        base_queryset = base_queryset.none()
                # Other roles (SuperAdmin, Admin, Analyst) see everything - no filtering needed
            except Exception:
                # If there's an exception and user is not superuser, show nothing
                if not self.request.user.is_superuser:
                    base_queryset = base_queryset.none()
        
        # Active programs count (non-archived, status active, with permission filters)
        context['active_programs_count'] = base_queryset.filter(status='active').count()
        
        # Assigned programs: programs with at least one active client enrollment
        # Active enrollment = is_archived=False, start_date <= today, and (end_date IS NULL OR end_date > today)
        active_enrollments = ClientProgramEnrollment.objects.filter(
            program=OuterRef('pk'),
            is_archived=False,
            start_date__lte=today
        ).filter(
            Q(end_date__isnull=True) | Q(end_date__gt=today)
        )
        assigned_queryset = base_queryset.annotate(has_active_enrollment=Exists(active_enrollments))
        context['assigned_programs_count'] = assigned_queryset.filter(has_active_enrollment=True).count()
        
        # Unassigned programs: programs with no active enrollments
        context['unassigned_programs_count'] = assigned_queryset.filter(has_active_enrollment=False).count()
        
        # Add filter options to context
        context['programs_with_capacity'] = programs_with_capacity
        context['total_filtered_count'] = total_filtered_count
        # Exclude archived departments and HASS from department dropdown (for non-admin users)
        # Always exclude archived departments
        departments_queryset = Department.objects.filter(is_archived=False)
        context['departments'] = departments_queryset.exclude(
            name__iexact='HASS'
        ).order_by('name')
        context['status_choices'] = Program.STATUS_CHOICES
        context['capacity_choices'] = [
            ('', 'All Programs'),
            ('at_capacity', 'At Capacity'),
            ('available', 'Has Available Spots'),
            ('no_limit', 'No Capacity Limit'),
        ]
        
        # Get accessible programs for dropdown filter
        context['all_programs'] = base_queryset.order_by('name')
        
        # Add current filter values
        context['current_department'] = self.request.GET.get('department', '')
        context['current_status'] = self.request.GET.get('status', '')
        context['current_program'] = self.request.GET.get('program', '')
        context['per_page'] = self.request.GET.get('per_page', '10')
        context['time_filter'] = time_filter
        
        # Force pagination to be enabled if there are any results
        if context.get('paginator') and context['paginator'].count > 0:
            context['is_paginated'] = True
        context['current_capacity'] = self.request.GET.get('capacity', '')
        context['search_query'] = self.request.GET.get('search', '')
        
        context['sort'] = self.request.GET.get('sort', 'name_asc')
        return context

@method_decorator(jwt_required, name='dispatch')
class ProgramDetailView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, DetailView):
    model = Program
    template_name = 'programs/program_detail.html'
    context_object_name = 'program'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    
    def get_queryset(self):
        """Only allow access to non-archived programs"""
        return super().get_queryset().filter(is_archived=False)
    
    def get(self, request, *args, **kwargs):
        """Override get method to handle permission checks before rendering"""
        try:
            # First, try to get the object
            self.object = self.get_object()
        except Exception:
            # If object doesn't exist, handle based on request type
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                from django.http import JsonResponse
                return JsonResponse({
                    'error': True,
                    'message': "You don't have access to this program. Only programs assigned to you are accessible.",
                    'type': 'permission_error'
                }, status=403)
            else:
                from django.shortcuts import redirect
                from django.urls import reverse
                return redirect(f"{reverse('core:permission_error')}?type=program_not_assigned&resource=program")
        
        # Check if user has access to this program
        if not request.user.is_superuser:
            try:
                staff = request.user.staff_profile
                if staff.is_program_manager():
                    assigned_programs = staff.get_assigned_programs()
                    if self.object not in assigned_programs:
                        # Handle based on request type
                        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                            from django.http import JsonResponse
                            return JsonResponse({
                                'error': True,
                                'message': f"You don't have access to {self.object.name}. Only programs assigned to you are accessible.",
                                'type': 'permission_error'
                            }, status=403)
                        else:
                            from django.shortcuts import redirect
                            from django.urls import reverse
                            return redirect(f"{reverse('core:permission_error')}?type=program_not_assigned&resource=program&name={self.object.name}")
                
                elif staff.is_leader():
                    # Leaders can only access programs from their assigned departments
                    from core.models import Department
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True,
                        is_archived=False
                    ).distinct()
                    assigned_programs = Program.objects.filter(
                        department__in=assigned_departments
                    ).distinct()
                    
                    if self.object not in assigned_programs:
                        # Handle based on request type
                        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                            from django.http import JsonResponse
                            return JsonResponse({
                                'error': True,
                                'message': f"You don't have access to {self.object.name}. Only programs in your assigned departments are accessible.",
                                'type': 'permission_error'
                            }, status=403)
                        else:
                            from django.shortcuts import redirect
                            from django.urls import reverse
                            return redirect(f"{reverse('core:permission_error')}?type=program_not_assigned&resource=program&name={self.object.name}")
            except Exception:
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    from django.http import JsonResponse
                    return JsonResponse({
                        'error': True,
                        'message': "You don't have permission to access this program.",
                        'type': 'permission_error'
                    }, status=403)
                else:
                    from django.shortcuts import redirect
                    from django.urls import reverse
                    return redirect(f"{reverse('core:permission_error')}?type=access_denied&resource=program")
        
        # If we get here, user has access, proceed with normal rendering
        context = self.get_context_data(object=self.object)
        
        # Check if this is an AJAX request for client list
        if request.GET.get('ajax') == '1':
            from django.template.loader import render_to_string
            client_list_html = render_to_string('programs/client_list_ajax.html', context)
            from django.http import HttpResponse
            return HttpResponse(client_list_html)
        
        return self.render_to_response(context)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        program = context.get('program') or self.object
        
        # Get current enrollments - only fetch first 4 initially for performance
        from core.models import ClientProgramEnrollment
        current_enrollments_queryset = ClientProgramEnrollment.objects.filter(
            program=program,
            start_date__lte=timezone.now().date()
        ).filter(
            models.Q(end_date__isnull=True) | models.Q(end_date__gt=timezone.now().date())
        ).filter(is_archived=False)
        current_enrollments_queryset = current_enrollments_queryset.select_related('client').order_by('-start_date')
        
        # Get total count for display
        total_enrollments_count = current_enrollments_queryset.count()
        
        # Only fetch first 4 enrollments initially
        initial_enrollments = list(current_enrollments_queryset[:4])
        
        # Keep full queryset for count and other uses
        current_enrollments = current_enrollments_queryset
        
        # Get program staff
        from core.models import ProgramStaff
        program_staff = ProgramStaff.objects.filter(program=program).select_related('staff')
        
        # Get program managers
        from core.models import ProgramManagerAssignment
        program_managers = ProgramManagerAssignment.objects.filter(
            program=program,
            is_active=True
        ).select_related('staff')
        
        # Get available clients (not currently enrolled in this program)
        from core.models import Client
        from django.core.paginator import Paginator
        
        enrolled_client_ids = current_enrollments.values_list('client_id', flat=True)
        available_clients_queryset = Client.objects.exclude(id__in=enrolled_client_ids)
        # Exclude archived clients for non-admin users
        if not can_see_archived(self.request.user):
            available_clients_queryset = available_clients_queryset.filter(is_archived=False)
        available_clients_queryset = available_clients_queryset.order_by('first_name', 'last_name')
        
        # Add pagination for available clients
        clients_per_page = self.request.GET.get('clients_per_page', '10')
        try:
            clients_per_page = int(clients_per_page)
            if clients_per_page not in [5, 10, 50, 100]:
                clients_per_page = 10
        except (ValueError, TypeError):
            clients_per_page = 10
            
        clients_paginator = Paginator(available_clients_queryset, clients_per_page)
        clients_page_number = self.request.GET.get('clients_page', 1)
        try:
            available_clients_page = clients_paginator.get_page(clients_page_number)
        except:
            available_clients_page = clients_paginator.get_page(1)
        
        available_clients = available_clients_page
        
        # Get available staff members who can be assigned as program managers
        from core.models import Staff
        
        assigned_manager_ids = program_managers.values_list('staff_id', flat=True)
        # Only show staff members who have the Manager role
        available_staff_queryset = Staff.objects.filter(
            staffrole__role__name='Manager'
        ).exclude(id__in=assigned_manager_ids).order_by('first_name', 'last_name').distinct()
        
        # Get all available staff (no pagination)
        available_staff = available_staff_queryset
        
        # Ensure all querysets are properly initialized (defensive programming)
        if current_enrollments is None:
            current_enrollments = ClientProgramEnrollment.objects.none()
        if program_staff is None:
            program_staff = ProgramStaff.objects.none()
        if program_managers is None:
            program_managers = ProgramManagerAssignment.objects.none()
        if available_clients is None:
            available_clients = Client.objects.none()
        if available_staff is None:
            available_staff = Staff.objects.none()
        
        # Get capacity information with defensive programming
        try:
            current_enrollments_count = program.get_current_enrollments_count()
        except Exception:
            current_enrollments_count = 0
            
        try:
            capacity_percentage = program.get_capacity_percentage()
        except Exception:
            capacity_percentage = 0.0
            
        try:
            available_capacity = program.get_available_capacity()
        except Exception:
            available_capacity = 0
            
        try:
            is_at_capacity = program.is_at_capacity()
        except Exception:
            is_at_capacity = False
        
        # Get enrollment history (last 30 days)
        from datetime import timedelta
        thirty_days_ago = timezone.now().date() - timedelta(days=30)
        try:
            recent_enrollments_queryset = ClientProgramEnrollment.objects.filter(
                program=program,
                start_date__gte=thirty_days_ago
            )
            # Exclude archived enrollments for non-admin users
            if not can_see_archived(self.request.user):
                recent_enrollments_queryset = recent_enrollments_queryset.filter(is_archived=False)
            recent_enrollments = recent_enrollments_queryset.select_related('client').order_by('-start_date')[:10]
        except Exception:
            recent_enrollments = ClientProgramEnrollment.objects.none()
        
        context.update({
            'current_enrollments': current_enrollments,
            'initial_enrollments': initial_enrollments,
            'total_enrollments_count': total_enrollments_count,
            'current_enrollments_count': current_enrollments_count,
            'capacity_percentage': capacity_percentage,
            'available_capacity': available_capacity,
            'is_at_capacity': is_at_capacity,
            'program_staff': program_staff,
            'program_managers': program_managers,
            'available_clients': available_clients,
            'available_staff': available_staff,
            'recent_enrollments': recent_enrollments,
            'clients_paginator': clients_paginator,
            'clients_page_obj': available_clients_page,
            'clients_per_page': clients_per_page,
        })
        
        return context


@csrf_protect
@require_http_methods(["GET"])
@login_required
def fetch_enrollments_ajax(request, external_id):
    """AJAX endpoint to fetch enrollments with pagination"""
    try:
        program = Program.objects.get(external_id=external_id, is_archived=False)
        
        # Check permissions
        if not request.user.is_superuser:
            try:
                staff = request.user.staff_profile
                if staff.is_program_manager():
                    assigned_programs = staff.get_assigned_programs()
                    if program not in assigned_programs:
                        return JsonResponse({'error': 'Access denied'}, status=403)
                elif staff.is_leader():
                    from core.models import Department
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True,
                        is_archived=False
                    ).distinct()
                    assigned_programs = Program.objects.filter(
                        department__in=assigned_departments
                    ).distinct()
                    if program not in assigned_programs:
                        return JsonResponse({'error': 'Access denied'}, status=403)
            except Exception:
                return JsonResponse({'error': 'Access denied'}, status=403)
        
        # Get pagination parameters
        offset = int(request.GET.get('offset', 0))
        limit = int(request.GET.get('limit', 4))
        
        # Get enrollments with pagination
        from core.models import ClientProgramEnrollment
        enrollments_queryset = ClientProgramEnrollment.objects.filter(
            program=program,
            start_date__lte=timezone.now().date()
        ).filter(
            models.Q(end_date__isnull=True) | models.Q(end_date__gt=timezone.now().date())
        ).filter(is_archived=False)
        enrollments_queryset = enrollments_queryset.select_related('client').order_by('-start_date')
        
        total_count = enrollments_queryset.count()
        
        # Get paginated enrollments
        enrollments = enrollments_queryset[offset:offset + limit]
        
        # Format enrollments for JSON response
        enrollments_data = []
        for enrollment in enrollments:
            enrollments_data.append({
                'id': enrollment.id,
                'client_id': enrollment.client.id,
                'client_name': f"{enrollment.client.first_name} {enrollment.client.last_name}",
                'client_initials': f"{enrollment.client.first_name[0] if enrollment.client.first_name else ''}{enrollment.client.last_name[0] if enrollment.client.last_name else ''}",
                'start_date': enrollment.start_date.strftime('%b %d, %Y') if enrollment.start_date else '',
                'status': enrollment.calculated_status,
                'status_display': enrollment.get_status_display(),
                'external_id': str(enrollment.client.external_id),
            })
        
        return JsonResponse({
            'success': True,
            'enrollments': enrollments_data,
            'has_more': (offset + limit) < total_count,
            'total_count': total_count,
            'offset': offset,
        })
        
    except Program.DoesNotExist:
        return JsonResponse({'error': 'Program not found'}, status=404)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@method_decorator(jwt_required, name='dispatch')
class ProgramCSVUploadView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, View):
    """Upload programs via CSV. Avoid duplicates by case-insensitive program name match."""

    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to upload programs"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Manager role users cannot upload programs
                if 'Manager' in role_names and not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to upload programs. Contact your administrator.')
                    return redirect('programs:list')
                
                # Leader role users cannot upload programs
                if 'Leader' in role_names and not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to upload programs. Contact your administrator.')
                    return redirect('programs:list')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)

    def post(self, request):
        try:
            if 'file' not in request.FILES:
                messages.error(request, 'No file provided')
                return redirect('programs:list')

            file = request.FILES['file']

            try:
                content = file.read()
                decoded = content.decode('utf-8', errors='ignore')
            except Exception:
                messages.error(request, 'Unable to read the uploaded file. Please upload a valid CSV file.')
                return redirect('programs:list')

            reader = csv.DictReader(io.StringIO(decoded))

            # Normalization helpers
            def normalize_header(h):
                # handle BOM and whitespace
                return (h or '').replace('\ufeff', '').strip().lower()

            def get_value(normalized_row, aliases):
                for a in aliases:
                    # exact
                    if a in normalized_row and str(normalized_row.get(a, '')).strip() != '':
                        return str(normalized_row[a]).strip()
                    # startswith fallback (e.g., "program n" in header sample)
                    for key in normalized_row.keys():
                        if key.startswith(a) and str(normalized_row.get(key, '')).strip() != '':
                            return str(normalized_row[key]).strip()
                return ''

            canonical = {
                'name': {'program name', 'program', 'name', 'program_n', 'program_name'},
                'department': {'department', 'dept', 'departme'},
                'location': {'location', 'site'},
                'status': {'status'},
                'capacity_current': {'capacity', 'current capacity', 'capacity_current'},
                'description': {'description', 'details'},
            }

            created_count = 0
            updated_count = 0
            skipped = 0
            errors = []

            # Helper: status mapping
            def map_status(value: str):
                v = (value or '').strip().lower()
                if v in ('active', 'a', '1', 'yes', 'y'): return 'active'
                if v in ('inactive', 'i', '0', 'no', 'n'): return 'inactive'
                if v in ('suggested', 's', 'proposed'): return 'suggested'
                return 'active'

            # Process rows
            for idx, row in enumerate(reader, start=2):
                try:
                    # Build a normalized row mapping once per row
                    normalized_row = {normalize_header(k): v for k, v in (row or {}).items()}

                    name = get_value(normalized_row, canonical['name'])
                    if not name:
                        skipped += 1
                        continue

                    # Normalize name (strip whitespace)
                    name = name.strip()

                    dept_name = get_value(normalized_row, canonical['department'])
                    location = get_value(normalized_row, canonical['location'])
                    status_val = map_status(get_value(normalized_row, canonical['status']) or 'active')
                    description = get_value(normalized_row, canonical['description'])

                    # Capacity parse
                    cap_raw = get_value(normalized_row, canonical['capacity_current'])
                    try:
                        cap_clean = str(cap_raw).replace(',', '').strip()
                        capacity_current = int(cap_clean) if cap_clean != '' else 0
                    except Exception:
                        capacity_current = 0

                    # Department (create if not exists)
                    if dept_name:
                        department, _ = Department.objects.get_or_create(name=dept_name.strip())
                    else:
                        department, _ = Department.objects.get_or_create(name='NA')

                    # Find existing program by case-insensitive name AND department (must match both)
                    # This ensures we don't create duplicates or match programs from wrong departments
                    program = Program.objects.filter(
                        name__iexact=name,
                        department=department
                    ).first()

                    if program:
                        # Update existing program (only update non-empty fields)
                        # Department is already correct since we matched by both name and department
                        if location and location.strip():
                            program.location = location.strip()
                        if status_val:
                            program.status = status_val
                        if description and description.strip():
                            program.description = description.strip()
                        program.capacity_current = capacity_current
                        # Audit
                        if request.user.is_authenticated:
                            program.updated_by = request.user.get_full_name() or request.user.username or request.user.email
                        program.save()
                        updated_count += 1
                    else:
                        # Create new program
                        created_by = 'System'
                        if request.user.is_authenticated:
                            created_by = request.user.get_full_name() or request.user.username or request.user.email or 'System'
                        Program.objects.create(
                            name=name,
                            department=department,
                            location=location or 'TBD',
                            status=status_val,
                            description=description,
                            capacity_current=capacity_current,
                            created_by=created_by,
                            updated_by=created_by,
                        )
                        created_count += 1
                except Exception as e:
                    errors.append(f"Row {idx}: {str(e)}")
                    skipped += 1

            msg = f"Upload complete. Created: {created_count}, Updated: {updated_count}, Skipped: {skipped}."
            if errors:
                messages.warning(request, msg + f" Errors: {len(errors)}")
            else:
                messages.success(request, msg)

            return redirect('programs:list')
        except Exception as e:
            messages.error(request, f"Upload failed: {str(e)}")
            return redirect('programs:list')


@method_decorator(jwt_required, name='dispatch')
class ProgramBulkEnrollView(ProgramManagerAccessMixin, View):
    """Handle bulk enrollment of clients in a program"""
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to enroll clients"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot enroll clients
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to enroll clients. Contact your administrator.')
                    return redirect('programs:list')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request, external_id):
        from core.models import Program, ClientProgramEnrollment
        from django.contrib import messages
        from django.shortcuts import redirect
        from django.utils import timezone
        from django.db import transaction
        
        try:
            program = Program.objects.get(external_id=external_id, is_archived=False)
            client_ids = request.POST.getlist('client_ids')
            start_date = request.POST.get('start_date', timezone.now().date())
            
            if not client_ids:
                messages.error(request, 'Please select at least one client to enroll.')
                return redirect('programs:detail', external_id=external_id)
            
            # Convert start_date string to date object if needed
            if isinstance(start_date, str):
                from datetime import datetime
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            
            enrolled_count = 0
            errors = []
            
            with transaction.atomic():
                for client_id in client_ids:
                    try:
                        from core.models import Client
                        client = Client.objects.get(id=client_id)
                        
                        # Check if client can be enrolled
                        can_enroll, message = program.can_enroll_client(client, start_date)
                        
                        if can_enroll:
                            # Create enrollment
                            enrollment = ClientProgramEnrollment.objects.create(
                                client=client,
                                program=program,
                                start_date=start_date,
                                status='active',
                                created_by=request.user.get_full_name() or request.user.username,
                                updated_by=request.user.get_full_name() or request.user.username
                            )
                            
                            # Create audit log entry for enrollment creation
                            try:
                                from core.models import create_audit_log
                                create_audit_log(
                                    entity_name='Enrollment',
                                    entity_id=enrollment.external_id,
                                    action='create',
                                    changed_by=request.user,
                                    diff_data={
                                        'client': str(enrollment.client),
                                        'program': str(enrollment.program),
                                        'start_date': str(enrollment.start_date),
                                        'status': enrollment.status,
                                        'created_by': enrollment.created_by,
                                        'source': 'program_detail_page'
                                    }
                                )
                            except Exception as e:
                                print(f"Error creating audit log for enrollment: {e}")
                            
                            enrolled_count += 1
                        else:
                            errors.append(f"{client.first_name} {client.last_name}: {message}")
                            
                    except Client.DoesNotExist:
                        errors.append(f"Client with ID {client_id} not found.")
                    except Exception as e:
                        errors.append(f"Error enrolling client {client_id}: {str(e)}")
            
            # Show success/error messages
            if enrolled_count > 0:
                messages.success(request, f'Successfully enrolled {enrolled_count} client(s) in {program.name}.')
            
            if errors:
                for error in errors:
                    messages.warning(request, error)
            
        except Program.DoesNotExist:
            messages.error(request, 'Program not found.')
            return redirect('programs:list')
        except Exception as e:
            messages.error(request, f'An error occurred: {str(e)}')
        
        return redirect(reverse('programs:detail', args=[external_id]) + '?enrolled=true')

@method_decorator(jwt_required, name='dispatch')
class ProgramBulkAssignManagersView(ProgramManagerAccessMixin, View):
    """Handle bulk assignment of program managers to a program"""
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to assign managers"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Only SuperAdmin users can assign managers
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to assign managers. Contact your administrator.')
                    return redirect('programs:list')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request, external_id):
        from core.models import Program, ProgramManagerAssignment, Staff
        from django.contrib import messages
        from django.shortcuts import redirect
        from django.db import transaction
        
        try:
            program = Program.objects.get(external_id=external_id, is_archived=False)
            staff_ids = request.POST.getlist('staff_ids')
            
            if not staff_ids:
                messages.error(request, 'Please select at least one staff member to assign as program manager.')
                return redirect('programs:detail', external_id=external_id)
            
            assigned_count = 0
            errors = []
            
            with transaction.atomic():
                for staff_id in staff_ids:
                    try:
                        staff = Staff.objects.get(id=staff_id)
                        
                        # Check if staff already assigned to this program
                        existing_assignment = ProgramManagerAssignment.objects.filter(
                            staff=staff,
                            program=program,
                            is_active=True
                        ).exists()
                        
                        if not existing_assignment:
                            # Create new assignment
                            assignment = ProgramManagerAssignment.objects.create(
                                staff=staff,
                                program=program,
                                assigned_by=request.user.staff_profile if hasattr(request.user, 'staff_profile') else None
                            )
                            assigned_count += 1
                        else:
                            errors.append(f"{staff.first_name} {staff.last_name} is already assigned as a program manager for this program.")
                            
                    except Staff.DoesNotExist:
                        errors.append(f"Staff member with ID {staff_id} not found.")
                    except Exception as e:
                        errors.append(f"Error assigning staff {staff_id}: {str(e)}")
            
            # Show success/error messages
            if assigned_count > 0:
                messages.success(request, f'Successfully assigned {assigned_count} program manager(s) to {program.name}.')
            
            if errors:
                for error in errors:
                    messages.warning(request, error)
            
        except Program.DoesNotExist:
            messages.error(request, 'Program not found.')
            return redirect('programs:list')
        except Exception as e:
            messages.error(request, f'An error occurred: {str(e)}')
        
        return redirect('programs:detail', external_id=external_id)

@method_decorator(jwt_required, name='dispatch')
class ProgramCreateView(ProgramManagerAccessMixin, CreateView):
    model = Program
    template_name = 'programs/program_form.html'
    fields = ['name', 'department', 'location', 'capacity_current', 'capacity_effective_date', 'no_capacity_limit', 'status']
    success_url = reverse_lazy('programs:list')
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to create programs"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot create programs
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to create programs. Contact your administrator.')
                    return redirect('programs:list')
                
                # Manager role users cannot create programs
                if 'Manager' in role_names and not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to create programs. Contact your administrator.')
                    return redirect('programs:list')
                
                # Leader role users cannot create programs
                if 'Leader' in role_names and not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to create programs. Contact your administrator.')
                    return redirect('programs:list')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_form(self, form_class=None):
        """Filter out HASS from department dropdown"""
        form = super().get_form(form_class)
        if 'department' in form.fields:
            # Always exclude archived departments
            departments_queryset = Department.objects.filter(is_archived=False)
            form.fields['department'].queryset = departments_queryset.exclude(
                name__iexact='HASS'
            ).order_by('name')
        return form
    
    def get_initial(self):
        """Set default values for the form"""
        initial = super().get_initial()
        # Get or create NA department
        na_department, _ = Department.objects.get_or_create(
            name='NA',
            defaults={'owner': 'System'}
        )
        initial['department'] = na_department
        initial['status'] = 'active'  # Set default status to active
        return initial
    
    def form_valid(self, form):
        """Handle program creation with audit logging"""
        no_capacity_limit = form.cleaned_data.get('no_capacity_limit')
        capacity_value = form.cleaned_data.get('capacity_current')

        if not no_capacity_limit:
            try:
                capacity_int = int(capacity_value)
            except (TypeError, ValueError):
                capacity_int = 0

            if capacity_int <= 0:
                form.add_error('capacity_current', 'Enter a capacity greater than 0 or select "No Capacity Limit".')
                return self.form_invalid(form)
        else:
            form.cleaned_data['capacity_current'] = 0

        program = form.save(commit=False)
        if program.no_capacity_limit:
            program.capacity_current = 0
        
        # Set created_by and updated_by fields
        if self.request.user.is_authenticated:
            first_name = self.request.user.first_name or ''
            last_name = self.request.user.last_name or ''
            user_name = f"{first_name} {last_name}".strip()
            if not user_name or user_name == ' ':
                user_name = self.request.user.username or self.request.user.email or 'System'
            program.created_by = user_name
            program.updated_by = user_name
        else:
            program.created_by = 'System'
            program.updated_by = 'System'
        
        program.save()
        
        # Create audit log entry for program creation
        try:
            from core.models import create_audit_log
            create_audit_log(
                entity_name='Program',
                entity_id=program.external_id,
                action='create',
                changed_by=self.request.user,
                diff_data={
                    'name': program.name,
                    'department': str(program.department),
                    'location': program.location or '',
                    'capacity_current': program.capacity_current,
                    'capacity_effective_date': str(program.capacity_effective_date) if program.capacity_effective_date else None,
                    'no_capacity_limit': program.no_capacity_limit,
                    'created_by': self.request.user.get_full_name() or self.request.user.username
                }
            )
        except Exception as e:
            print(f"Error creating audit log for program creation: {e}")
        
        create_success(self.request, 'Program')
        return super().form_valid(form)

@method_decorator(jwt_required, name='dispatch')
class ProgramUpdateView(ProgramManagerAccessMixin, UpdateView):
    model = Program
    template_name = 'programs/program_form.html'
    fields = ['name', 'department', 'location', 'capacity_current', 'capacity_effective_date', 'no_capacity_limit', 'status']
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('programs:list')
    
    def get_queryset(self):
        """Restrict updates to non-archived programs"""
        return super().get_queryset().filter(is_archived=False)
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to edit programs"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot edit programs
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to edit programs. Contact your administrator.')
                    return redirect('programs:list')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_object(self, queryset=None):
        """Override to ensure program managers can only edit their assigned programs"""
        obj = super().get_object(queryset)
        
        # The ProgramManagerAccessMixin should have already filtered the queryset
        # But let's double-check access here for extra security
        if not self.request.user.is_superuser:
            try:
                staff = self.request.user.staff_profile
                if staff.is_program_manager():
                    assigned_programs = staff.get_assigned_programs()
                    if obj not in assigned_programs:
                        from django.http import Http404
                        raise Http404("Program not found or access denied")
            except Exception:
                from django.http import Http404
                raise Http404("Program not found or access denied")
        
        return obj
    
    def get_form(self, form_class=None):
        """Filter out HASS from department dropdown"""
        form = super().get_form(form_class)
        if 'department' in form.fields:
            # Always exclude archived departments
            departments_queryset = Department.objects.filter(is_archived=False)
            form.fields['department'].queryset = departments_queryset.exclude(
                name__iexact='HASS'
            ).order_by('name')
        return form
    
    def form_valid(self, form):
        """Handle program updates with audit logging"""
        # Get the original program data before saving
        original_program = self.get_object()
        
        # Prevent Managers and Leaders from changing department
        if not self.request.user.is_superuser:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Only SuperAdmin and Admin can change departments
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    # Keep the original department value
                    form.cleaned_data['department'] = original_program.department
            except Exception:
                pass
        
        no_capacity_limit = form.cleaned_data.get('no_capacity_limit')
        capacity_value = form.cleaned_data.get('capacity_current')

        if not no_capacity_limit:
            try:
                capacity_int = int(capacity_value)
            except (TypeError, ValueError):
                capacity_int = 0

            if capacity_int <= 0:
                form.add_error('capacity_current', 'Enter a capacity greater than 0 or select "No Capacity Limit".')
                return self.form_invalid(form)
        else:
            form.cleaned_data['capacity_current'] = 0
        
        # Set the updated_by field before saving
        program = form.save(commit=False)
        if program.no_capacity_limit:
            program.capacity_current = 0
        
        # Set updated_by field
        if self.request.user.is_authenticated:
            first_name = self.request.user.first_name or ''
            last_name = self.request.user.last_name or ''
            user_name = f"{first_name} {last_name}".strip()
            if not user_name or user_name == ' ':
                user_name = self.request.user.username or self.request.user.email or 'System'
            program.updated_by = user_name
        else:
            program.updated_by = 'System'
        
        program.save()
        
        # Create audit log entry for program update
        try:
            from core.models import create_audit_log
            
            # Compare original and updated values to detect changes
            changes = {}
            
            # Check each field for changes
            if original_program.name != program.name:
                changes['name'] = f"{original_program.name} → {program.name}"
            
            if original_program.department != program.department:
                changes['department'] = f"{str(original_program.department)} → {str(program.department)}"
            
            if original_program.location != program.location:
                changes['location'] = f"{original_program.location or ''} → {program.location or ''}"
            
            if original_program.capacity_current != program.capacity_current:
                changes['capacity_current'] = f"{original_program.capacity_current} → {program.capacity_current}"
            
            if original_program.no_capacity_limit != program.no_capacity_limit:
                changes['no_capacity_limit'] = f"{original_program.no_capacity_limit} → {program.no_capacity_limit}"
            
            if original_program.capacity_effective_date != program.capacity_effective_date:
                changes['capacity_effective_date'] = f"{original_program.capacity_effective_date} → {program.capacity_effective_date}"
            
            # Only create audit log if there were changes
            if changes:
                create_audit_log(
                    entity_name='Program',
                    entity_id=program.external_id,
                    action='update',
                    changed_by=self.request.user,
                    diff_data=changes
                )
        except Exception as e:
            print(f"Error creating audit log for program update: {e}")
        
        update_success(self.request, 'Program')
        return super().form_valid(form)

@method_decorator(jwt_required, name='dispatch')
class ProgramDeleteView(ProgramManagerAccessMixin, DeleteView):
    model = Program
    template_name = 'programs/program_confirm_delete.html'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('programs:list')
    
    def get_queryset(self):
        """Restrict deletes to non-archived programs"""
        return super().get_queryset().filter(is_archived=False)
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to delete programs"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Only SuperAdmin and Admin can delete programs
                # Managers and Leaders cannot delete programs
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to delete programs. Contact your administrator.')
                    return redirect('programs:list')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_object(self, queryset=None):
        """Override to ensure program managers can only delete their assigned programs"""
        obj = super().get_object(queryset)
        
        # The ProgramManagerAccessMixin should have already filtered the queryset
        # But let's double-check access here for extra security
        if not self.request.user.is_superuser:
            try:
                staff = self.request.user.staff_profile
                if staff.is_program_manager():
                    assigned_programs = staff.get_assigned_programs()
                    if obj not in assigned_programs:
                        from django.http import Http404
                        raise Http404("Program not found or access denied")
            except Exception:
                from django.http import Http404
                raise Http404("Program not found or access denied")
        
        return obj
    
    def form_valid(self, form):
        """Handle program deletion with audit logging"""
        # Get the program object first
        program = self.get_object()
        
        # Create audit log entry before deletion
        try:
            from core.models import create_audit_log
            create_audit_log(
                entity_name='Program',
                entity_id=program.external_id,
                action='delete',
                changed_by=self.request.user,
                diff_data={
                    'name': program.name,
                    'department': str(program.department),
                    'location': program.location or '',
                    'capacity_current': program.capacity_current,
                    'capacity_effective_date': str(program.capacity_effective_date) if program.capacity_effective_date else None,
                    'deleted_by': f"{self.request.user.first_name} {self.request.user.last_name}".strip() or self.request.user.username
                }
            )
        except Exception as e:
            print(f"Error creating audit log for program deletion: {e}")
        
        # Soft delete: set is_archived=True and archived_at timestamp
        from django.utils import timezone
        program.is_archived = True
        program.archived_at = timezone.now()
        user_name = f"{self.request.user.first_name} {self.request.user.last_name}".strip() or self.request.user.username
        program.updated_by = user_name
        program.save()

        # Also archive enrollments associated with this program
        from core.models import ClientProgramEnrollment
        now_ts = timezone.now()
        ClientProgramEnrollment.objects.filter(program=program, is_archived=False).update(
            is_archived=True,
            archived_at=now_ts,
            updated_by=user_name
        )
        
        delete_success(self.request, 'Program', program.name)
        messages.success(
            self.request, 
            f'Program {program.name} has been archived. You can restore it from the archived programs section.'
        )
        return redirect(self.success_url)

@method_decorator(jwt_required, name="dispatch")
class ProgramCSVExportView(ProgramManagerAccessMixin, ListView):
    """Export programs to CSV with filtering support"""
    model = Program
    template_name = "programs/program_list.html"
    
    def get_queryset(self):
        # Use the same filtering logic as ProgramListView
        queryset = super().get_queryset()
        # Always exclude archived programs - archived programs should not be shown
        queryset = queryset.filter(is_archived=False)
        # Exclude programs with archived departments for non-admin users
        if not can_see_archived(self.request.user):
            queryset = queryset.filter(department__is_archived=False)
        # Exclude programs assigned to HASS department (deleted department)
        queryset = queryset.exclude(department__name__iexact='HASS')
        
        # For staff-only users, apply the same filtering as ProgramListView
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    # Staff-only users see ONLY programs where their assigned clients are enrolled
                    from staff.models import StaffClientAssignment
                    
                    # Get programs where their assigned clients are enrolled
                    assigned_client_ids = StaffClientAssignment.objects.filter(
                        staff=staff,
                        is_active=True
                    ).values_list('client_id', flat=True)
                    
                    if assigned_client_ids:
                        queryset = queryset.filter(
                            clientprogramenrollment__client_id__in=assigned_client_ids
                        ).distinct()
                    else:
                        # If no client assignments, show no programs
                        queryset = queryset.none()
            except Exception:
                pass
        
        # Apply additional filters
        department_filter = self.request.GET.get("department", "")
        status_filter = self.request.GET.get("status", "")
        capacity_filter = self.request.GET.get("capacity", "")
        search_query = self.request.GET.get("search", "").strip()
        
        if department_filter:
            queryset = queryset.filter(department__name=department_filter)
        
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        
        if capacity_filter:
            if capacity_filter == "at_capacity":
                # Filter programs that are at or over capacity
                queryset = [p for p in queryset if p.is_at_capacity()]
            elif capacity_filter == "available":
                # Filter programs that have available capacity
                queryset = [p for p in queryset if not p.is_at_capacity()]
        
        if search_query:
            queryset = queryset.filter(
                models.Q(name__icontains=search_query) |
                models.Q(description__icontains=search_query) |
                models.Q(location__icontains=search_query) |
                models.Q(department__name__icontains=search_query)
            )
        
        return queryset.order_by("-created_at")
    
    def get(self, request, *args, **kwargs):
        # Create CSV response
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = "attachment; filename=\"programs_export.csv\""
        
        # Create CSV writer
        writer = csv.writer(response)
        
        # Write header row
        writer.writerow([
            "Program Name",
            "Department",
            "Location",
            "Status",
            "Capacity",
            "Current Enrollments",
            "Capacity Percentage",
            "Description",
            "Created At",
            "Updated At",
            "Current Enrollments (Client Names)",
            "Program Staff (Manager Names)"
        ])
        
        # Get filtered programs
        programs = self.get_queryset()
        
        # Write data rows
        for program in programs:
            # Get current enrollments
            current_enrollments_queryset = ClientProgramEnrollment.objects.filter(
                program=program
            )
            # Exclude archived enrollments for non-admin users
            if not can_see_archived(self.request.user):
                current_enrollments_queryset = current_enrollments_queryset.filter(is_archived=False)
            current_enrollments = current_enrollments_queryset.select_related("client")
            
            # Get program staff/managers
            program_managers = ProgramManagerAssignment.objects.filter(
                program=program
            ).select_related("staff")
            
            # Calculate capacity percentage
            capacity_percentage = 0
            if program.capacity_current > 0:
                current_count = current_enrollments.count()
                capacity_percentage = min(100, (current_count / program.capacity_current) * 100)
            
            # Create comma-separated lists
            client_names = ", ".join([
                f"{enrollment.client.first_name} {enrollment.client.last_name}"
                for enrollment in current_enrollments
            ])
            
            manager_names = ", ".join([
                f"{assignment.staff.first_name} {assignment.staff.last_name}"
                for assignment in program_managers
            ])
            
            writer.writerow([
                program.name,
                program.department.name,
                program.location or "",
                program.get_status_display(),
                program.capacity_current if program.capacity_current > 0 else "No limit",
                current_enrollments.count(),
                f"{capacity_percentage:.1f}%",
                program.description or "",
                program.created_at.strftime("%Y-%m-%d %H:%M:%S") if program.created_at else "",
                program.updated_at.strftime("%Y-%m-%d %H:%M:%S") if program.updated_at else "",
                client_names,
                manager_names
            ])
        
        return response

@method_decorator(jwt_required, name='dispatch')
class ProgramBulkChangeDepartmentView(ProgramManagerAccessMixin, View):
    """Handle bulk department change for programs"""
    
    def dispatch(self, request, *args, **kwargs):
        # Check if user has permission to change departments
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Only SuperAdmin and Admin can change departments
                # Managers and Leaders cannot change departments
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    return JsonResponse({
                        'success': False,
                        'error': 'You do not have permission to change program departments.'
                    }, status=403)
            except Exception:
                return JsonResponse({
                    'success': False,
                    'error': 'Unable to verify user permissions.'
                }, status=403)
        
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request):
        from core.models import Program, Department
        from django.contrib import messages
        from django.db import transaction
        from django.shortcuts import redirect
        import json
        
        try:
            # Get data from form POST
            program_ids_json = request.POST.get('program_ids', '[]')
            new_department_id = request.POST.get('new_department_id', '')
            
            # Parse program IDs from JSON string
            try:
                program_ids = json.loads(program_ids_json)
            except json.JSONDecodeError:
                program_ids = []
            
            if not program_ids:
                messages.error(request, 'No programs selected for department change.')
                return redirect('programs:list')
            
            if not new_department_id:
                messages.error(request, 'No department selected.')
                return redirect('programs:list')
            
            # Get the new department (exclude archived)
            try:
                new_department = Department.objects.get(id=new_department_id, is_archived=False)
            except Department.DoesNotExist:
                messages.error(request, 'Selected department does not exist.')
                return redirect('programs:list')
            
            # Get programs to update
            programs_to_update = Program.objects.filter(
                external_id__in=program_ids,
                is_archived=False
            )
            
            if not programs_to_update.exists():
                messages.error(request, 'No valid programs found to update.')
                return redirect('programs:list')
            
            updated_count = 0
            errors = []
            
            with transaction.atomic():
                for program in programs_to_update:
                    try:
                        old_department = program.department
                        program.department = new_department
                        program.save()
                        updated_count += 1
                    except Exception as e:
                        errors.append(f"Failed to update {program.name}: {str(e)}")
            
            if updated_count > 0:
                messages.success(request, f'Successfully updated department for {updated_count} program(s) to {new_department.name}.')
                
                # Show warnings for any errors that occurred
                if errors:
                    for error in errors:
                        messages.warning(request, error)
            else:
                messages.error(request, 'No programs were updated.')
                if errors:
                    for error in errors:
                        messages.warning(request, error)
            
            return redirect('programs:list')
                
        except Exception as e:
            messages.error(request, f'An error occurred: {str(e)}')
            return redirect('programs:list')


@method_decorator(jwt_required, name='dispatch')
class ProgramBulkDeleteView(ProgramManagerAccessMixin, View):
    """Handle bulk deletion of programs"""
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to delete programs"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Only SuperAdmin and Admin users can bulk delete programs
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.http import JsonResponse
                    return JsonResponse({
                        'success': False,
                        'error': 'You do not have permission to delete programs. Contact your administrator.'
                    }, status=403)
            except Exception:
                from django.http import JsonResponse
                return JsonResponse({
                    'success': False,
                    'error': 'You do not have permission to delete programs.'
                }, status=403)
        
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request):
        from core.models import Program, ClientProgramEnrollment, ProgramManagerAssignment
        from django.http import JsonResponse
        from django.db import transaction
        import json
        
        try:
            # Parse JSON data from request body
            data = json.loads(request.body)
            program_ids = data.get('program_ids', [])
            
            if not program_ids:
                return JsonResponse({
                    'success': False,
                    'error': 'No programs selected for deletion.'
                })
            
            # Get programs to delete
            programs_to_delete = Program.objects.filter(
                external_id__in=program_ids,
                is_archived=False
            )
            
            if not programs_to_delete.exists():
                return JsonResponse({
                    'success': False,
                    'error': 'No valid programs found to delete.'
                })
            
            deleted_count = 0
            errors = []
            
            with transaction.atomic():
                for program in programs_to_delete:
                    try:
                        # Create audit log entry before deletion
                        try:
                            from core.models import create_audit_log
                            create_audit_log(
                                entity_name='Program',
                                entity_id=program.external_id,
                                action='delete',
                                changed_by=request.user,
                                diff_data={
                                    'name': program.name,
                                    'department': str(program.department),
                                    'location': program.location or '',
                                    'capacity_current': program.capacity_current,
                                    'capacity_effective_date': str(program.capacity_effective_date) if program.capacity_effective_date else None,
                                    'deleted_by': f"{request.user.first_name} {request.user.last_name}".strip() or request.user.username,
                                    'source': 'bulk_delete'
                                }
                            )
                        except Exception as e:
                            print(f"Error creating audit log for program deletion: {e}")
                        
                        # Soft delete: archive program instead of actually deleting
                        # Note: We don't delete related enrollments/assignments - they remain but the program is archived
                        program.is_archived = True
                        program.archived_at = timezone.now()
                        program.updated_by = f"{request.user.first_name} {request.user.last_name}".strip() or request.user.username
                        program.save()
                        # Archive enrollments associated with this program
                        ClientProgramEnrollment.objects.filter(program=program, is_archived=False).update(
                            is_archived=True,
                            archived_at=timezone.now(),
                            updated_by=program.updated_by
                        )
                        deleted_count += 1
                        
                    except Exception as e:
                        errors.append(f"Error deleting {program.name}: {str(e)}")
            
            if deleted_count > 0:
                return JsonResponse({
                    'success': True,
                    'deleted_count': deleted_count,
                    'message': f'Successfully archived {deleted_count} program(s). You can restore them from the archived programs section.'
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': 'Failed to delete any programs.',
                    'errors': errors
                })
                
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON data.'
            })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'An error occurred: {str(e)}'
            })


@method_decorator(jwt_required, name="dispatch")
class ProgramBulkRestoreView(ProgramManagerAccessMixin, View):
    """Handle bulk restoration of archived programs"""
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to restore programs"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Only SuperAdmin and Admin users can bulk restore programs
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.http import JsonResponse
                    return JsonResponse({
                        'success': False,
                        'error': 'You do not have permission to restore programs.'
                    }, status=403)
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request):
        """Restore archived programs"""
        try:
            import json
            data = json.loads(request.body)
            program_ids = data.get('program_ids', [])
            
            if not program_ids:
                return JsonResponse({
                    'success': False,
                    'error': 'No programs selected for restoration'
                })
            
            # Get archived programs to restore
            programs_to_restore = Program.objects.filter(
                external_id__in=program_ids,
                is_archived=True
            )
            
            if not programs_to_restore.exists():
                return JsonResponse({
                    'success': False,
                    'error': 'No archived programs found with the provided IDs.'
                })
            
            restored_count = 0
            errors = []
            
            with transaction.atomic():
                for program in programs_to_restore:
                    try:
                        # Create audit log entry for restoration
                        try:
                            from core.models import create_audit_log
                            create_audit_log(
                                entity_name='Program',
                                entity_id=program.external_id,
                                action='restore',
                                changed_by=request.user,
                                diff_data={
                                    'name': program.name,
                                    'department': str(program.department),
                                    'restored_by': f"{request.user.first_name} {request.user.last_name}".strip() or request.user.username,
                                    'source': 'bulk_restore'
                                }
                            )
                        except Exception as e:
                            print(f"Error creating audit log for program restoration: {e}")
                        
                        # Restore: set is_archived=False and clear archived_at
                        program.is_archived = False
                        program.archived_at = None
                        program.updated_by = f"{request.user.first_name} {request.user.last_name}".strip() or request.user.username
                        program.save()
                        restored_count += 1
                        
                    except Exception as e:
                        errors.append(f"Error restoring {program.name}: {str(e)}")
            
            if restored_count > 0:
                return JsonResponse({
                    'success': True,
                    'restored_count': restored_count,
                    'message': f'Successfully restored {restored_count} program(s).'
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': 'Failed to restore any programs.',
                    'errors': errors
                })
                
        except json.JSONDecodeError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid JSON data.'
            })
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': f'An error occurred: {str(e)}'
            })
@csrf_protect
@require_http_methods(["POST"])
@login_required
def toggle_program_status(request, external_id):
    """Toggle program status between active and inactive (only for SuperAdmin/Admin)"""
    try:
        program = Program.objects.get(external_id=external_id, is_archived=False)
    except Program.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Program not found'}, status=404)
    
    # Check if user is SuperAdmin or Admin
    if not request.user.is_authenticated:
        return JsonResponse({'success': False, 'error': 'Authentication required'}, status=401)
    
    try:
        staff = request.user.staff_profile
        user_roles = staff.staffrole_set.select_related('role').all()
        role_names = [staff_role.role.name for staff_role in user_roles]
        
        if 'SuperAdmin' not in role_names and 'Admin' not in role_names:
            return JsonResponse({'success': False, 'error': 'Permission denied. Only SuperAdmin and Admin can change program status.'}, status=403)
    except Exception:
        return JsonResponse({'success': False, 'error': 'Permission denied'}, status=403)
    
    # Toggle status: active <-> inactive (if suggested, make it active first)
    if program.status == 'active':
        new_status = 'inactive'
    elif program.status == 'inactive':
        new_status = 'active'
    else:  # suggested
        new_status = 'active'
    
    # Update program status
    program.status = new_status
    user_name = request.user.get_full_name() or request.user.username
    program.updated_by = user_name
    program.save()
    
    return JsonResponse({
        'success': True,
        'message': f'Program status updated to {new_status}',
        'status': program.status,
        'status_display': program.get_status_display()
    })
