from django.shortcuts import render, redirect
from django.db.models import Count, Q, Case, When, Value, CharField, F, Exists, OuterRef
from django.db import models
from django.http import JsonResponse, HttpResponse
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from functools import wraps
import json
import csv
import uuid
import logging
from django.contrib import messages
from .message_utils import success_message, error_message, warning_message, info_message, create_success, update_success, delete_success, validation_error, permission_error, not_found_error
from django.urls import reverse_lazy, reverse
from django.views.generic import ListView, DetailView, CreateView, UpdateView, DeleteView, View
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.utils.decorators import method_decorator
from datetime import datetime, timedelta
from django.utils import timezone
from django.core.paginator import Paginator
from .models import (
    Client,
    Program,
    Staff,
    ClientProgramEnrollment,
    Department,
    ServiceRestriction,
    AuditLog,
    Notification,
)
from .forms import EnrollmentForm
from .forms import UserProfileForm, StaffProfileForm, PasswordChangeForm, ServiceRestrictionForm
from .notification_utils import create_service_restriction_notification


User = get_user_model()


def can_see_archived(user):
    """
    Helper function to check if a user can see archived items.
    Only SuperAdmin and Admin can see archived items.
    Returns True if user is SuperAdmin or Admin, False otherwise.
    """
    if not user or not user.is_authenticated:
        return False
    
    try:
        staff = user.staff_profile
        user_roles = staff.staffrole_set.select_related('role').all()
        role_names = [staff_role.role.name for staff_role in user_roles]
        return any(role in ['SuperAdmin', 'Admin'] for role in role_names)
    except Exception:
        return False


def get_active_clients_count(request, assigned_programs=None, is_program_manager=False, is_leader=False, is_staff_only=False, is_analyst=False):
    """
    Calculate active clients count using the same logic as the clients list view.
    Active clients are defined by status only: is_inactive=False
    
    Active clients are:
    - Clients that are not archived (is_archived=False)
    - Clients that are not marked as duplicates (duplicate_of__status != 'pending')
    - Clients that are not marked as inactive (is_inactive=False)
    """
    # Start with base queryset - exclude archived clients for non-admin users, duplicates, and inactive clients
    base_queryset = Client.objects.filter(is_inactive=False)
    # Exclude archived clients for non-admin users
    if not can_see_archived(request.user):
        base_queryset = base_queryset.filter(is_archived=False)
    base_queryset = base_queryset.exclude(duplicate_of__status='pending')
    
    # Apply permission filters based on user role
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            if is_program_manager and assigned_programs:
                # Program managers see clients enrolled in their assigned programs
                relationship_filters = Q()
                relationship_filters |= Q(clientprogramenrollment__program__in=assigned_programs)
                staff_name = f"{staff.user.first_name} {staff.user.last_name}".strip() or staff.user.username
                relationship_filters |= Q(clientprogramenrollment__created_by=staff_name)
                relationship_filters |= Q(servicerestriction__created_by=staff_name)
                relationship_filters |= Q(updated_by=staff_name)
                base_queryset = base_queryset.filter(relationship_filters)
            elif is_staff_only:
                # Staff-only users see clients assigned to them or enrolled in their assigned programs
                from staff.models import StaffProgramAssignment, StaffClientAssignment
                relationship_filters = Q()
                assigned_program_ids = StaffProgramAssignment.objects.filter(
                    staff=staff,
                    is_active=True
                ).values_list('program_id', flat=True)
                if assigned_program_ids:
                    relationship_filters |= Q(clientprogramenrollment__program_id__in=assigned_program_ids)
                assigned_client_ids = StaffClientAssignment.objects.filter(
                    staff=staff,
                    is_active=True
                ).values_list('client_id', flat=True)
                if assigned_client_ids:
                    relationship_filters |= Q(id__in=assigned_client_ids)
                if relationship_filters:
                    base_queryset = base_queryset.filter(relationship_filters)
                else:
                    base_queryset = base_queryset.none()
            elif is_leader and assigned_programs:
                # Leaders see clients enrolled in programs in their assigned departments
                base_queryset = base_queryset.filter(
                    clientprogramenrollment__program__in=assigned_programs
                )
            # Analysts and SuperAdmin see all clients (no additional filtering needed)
        except Exception:
            pass
    
    # Count active clients (is_inactive=False)
    active_clients_count = base_queryset.values('id').distinct().count()
    
    return active_clients_count


def get_date_range_filter(request):
    """Helper function to get date range filter parameters from request"""
    start_date = request.GET.get('start_date', '').strip()
    end_date = request.GET.get('end_date', '').strip()
    
    # Parse dates if provided
    parsed_start_date = None
    parsed_end_date = None
    
    if start_date:
        try:
            parsed_start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        except ValueError:
            parsed_start_date = None
    
    if end_date:
        try:
            parsed_end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError:
            parsed_end_date = None
    
    return start_date, end_date, parsed_start_date, parsed_end_date


class AnalystAccessMixin:
    """Mixin to block Analyst users from accessing individual pages"""
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user is Analyst and block access to individual pages"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Block Analyst users from accessing individual pages
                if 'Analyst' in role_names and not any(role in ['SuperAdmin', 'Manager', 'Leader', 'Staff'] for role in role_names):
                    messages.error(request, 'Analyst users can only access Dashboard and Reports. Individual pages are not accessible.')
                    return redirect('dashboard')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)


class StaffAccessControlMixin:
    """Mixin to restrict staff-only users to approved views."""

    allow_staff_access = False

    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                if staff.is_staff_only() and not self.allow_staff_access:
                    messages.error(request, 'You do not have permission to access this area.')
                    return redirect('core:restrictions')
            except Staff.DoesNotExist:
                pass
        return super().dispatch(request, *args, **kwargs)


class ProgramManagerAccessMixin:
    """Mixin to filter data for Managers based on their assigned programs"""
    
    def get_queryset(self):
        """Filter queryset based on user's assigned programs"""
        # Start with the base queryset with proper ordering and select_related
        queryset = self.model.objects.all().order_by('-created_at')
        
        # Add appropriate select_related based on model
        if hasattr(self.model, 'department'):
            queryset = queryset.select_related('department')
        elif hasattr(self.model, 'client') and hasattr(self.model, 'program'):
            queryset = queryset.select_related('client', 'program')
        elif hasattr(self.model, 'client'):
            queryset = queryset.select_related('client')
        elif hasattr(self.model, 'program'):
            queryset = queryset.select_related('program')
        
        if not self.request.user.is_authenticated:
            return queryset.none()
        
        # Superadmin can see everything - bypass all filtering
        if self.request.user.is_superuser:
            return queryset
        
        try:
            staff = self.request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            # Manager can see all data - no filtering for managers
            if staff.is_program_manager():
                # Managers see all restrictions without any filtering
                if self.model.__name__ == 'ServiceRestriction':
                    return queryset
                else:
                    # For other models, managers see only their assigned programs
                    assigned_programs = staff.get_assigned_programs()
                    if hasattr(self.model, 'program'):
                        return queryset.filter(program__in=assigned_programs)
                    else:
                        return queryset.filter(id__in=assigned_programs)
            
            # Leader can see data for their assigned departments
            elif staff.is_leader():
                # Use direct queries since the methods aren't working
                assigned_departments = Department.objects.filter(
                    leader_assignments__staff=staff,
                    leader_assignments__is_active=True,
                    is_archived=False
                ).distinct()
                assigned_programs = Program.objects.filter(
                    department__in=assigned_departments
                ).distinct()
                
                if self.model.__name__ == 'ServiceRestriction':
                    # Leaders see ALL restrictions (like Staff users)
                    return queryset
                elif hasattr(self.model, 'program'):
                    return queryset.filter(program__in=assigned_programs)
                elif hasattr(self.model, 'department'):
                    return queryset.filter(department__in=assigned_departments)
                elif hasattr(self.model, 'client'):
                    # For client models, show clients enrolled in assigned programs
                    assigned_clients = Client.objects.filter(
                        clientprogramenrollment__program__in=assigned_programs
                    ).distinct()
                    return queryset.filter(id__in=assigned_clients)
                else:
                    return queryset.filter(id__in=assigned_programs)
            
            # Staff users (including those with multiple roles) see limited access to data
            elif 'Staff' in role_names:
                from staff.models import StaffClientAssignment
                # Get programs where assigned clients are enrolled
                assigned_client_ids = StaffClientAssignment.objects.filter(
                    staff=staff,
                    is_active=True
                ).values_list('client_id', flat=True)
                assigned_programs = Program.objects.filter(
                    clientprogramenrollment__client_id__in=assigned_client_ids
                ).distinct()
                
                # Filter based on model type - special handling for different models
                if self.model.__name__ == 'ServiceRestriction':
                    # Staff users see ALL restrictions regardless of program or client assignments
                    return queryset
                elif hasattr(self.model, 'program') and self.model.__name__ == 'ClientProgramEnrollment':
                    # For ClientProgramEnrollment - Staff users see only enrollments for their assigned programs
                    return queryset.filter(program__in=assigned_programs)
                elif hasattr(self.model, 'program'):
                    # For other models with program field (like ServiceRestriction) - show all
                    return queryset
                elif hasattr(self.model, 'client') and not hasattr(self.model, 'program'):
                    # For models with only client field (like Client)
                    # Staff users see only clients enrolled in their assigned programs
                    assigned_clients = Client.objects.filter(
                        clientprogramenrollment__program__in=assigned_programs
                    ).distinct()
                    return queryset.filter(id__in=assigned_clients)
                else:
                    # For models without program field (like Program)
                    return queryset.filter(id__in=assigned_programs)
            
            # Other roles see everything (SuperAdmin, etc.)
            return queryset
            
        except Exception as e:
            # For superadmin, return all programs even if there's an exception
            if self.request.user.is_superuser:
                return queryset
            return queryset.none()
    

def jwt_required(view_func):
    """Decorator to require authentication (JWT or Django session)"""
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        # Check if user is authenticated (either JWT via middleware or Django session)
        if request.user.is_authenticated:
            return view_func(request, *args, **kwargs)
        
        # If no authentication at all, redirect to home
        return redirect('home')
    
    return wrapper


def home(request):
    """Home view that redirects authenticated users to restrictions page"""
    # Check if user is authenticated (either JWT via middleware or Django session)
    if request.user.is_authenticated:
        return redirect('core:restrictions')
    
    return render(request, 'home.html')


def dashboard(request):
    """Dashboard view - redirects to profile for users without proper permissions"""
    # Check if user is authenticated (either JWT via middleware or Django session)
    if not request.user.is_authenticated:
        return redirect('home')
    
    # Check if user has proper permissions to access dashboard
    try:
        staff_profile = request.user.staff_profile
        user_roles = staff_profile.staffrole_set.select_related('role').all()
        role_names = [staff_role.role.name for staff_role in user_roles]
        
        # Check if user has any meaningful permissions
        has_permissions = any(role in ['SuperAdmin', 'Admin', 'Staff', 'Manager', 'Leader', 'Analyst'] for role in role_names)
        
        if not has_permissions:
            # User doesn't have proper permissions, redirect to profile
            return redirect('core:profile')
            
    except Staff.DoesNotExist:
        # User doesn't have staff profile, redirect to profile
        return redirect('core:profile')
    
    # User has proper permissions, show dashboard
    # Check if user is a program manager, staff-only, leader, or analyst to filter data accordingly
    is_program_manager = False
    is_staff_only = False
    is_leader = False
    is_analyst = False
    assigned_programs = None
    assigned_clients = None
    staff_assigned_clients = None
    
    try:
        staff_profile = request.user.staff_profile
        user_roles = staff_profile.staffrole_set.select_related('role').all()
        role_names = [staff_role.role.name for staff_role in user_roles]
        
        if staff_profile.is_program_manager():
            is_program_manager = True
            assigned_programs = staff_profile.get_assigned_programs()
        elif staff_profile.is_leader():
            is_leader = True
            # Get assigned programs for leader users via departments using direct queries
            assigned_departments = Department.objects.filter(
                leader_assignments__staff=staff_profile,
                leader_assignments__is_active=True,
                is_archived=False
            ).distinct()
            assigned_programs = Program.objects.filter(
                department__in=assigned_departments
            ).distinct()
            # Get clients enrolled in assigned programs
            assigned_clients = Client.objects.filter(
                clientprogramenrollment__program__in=assigned_programs
            ).distinct()
        elif 'Analyst' in role_names:
            is_analyst = True
            # Analysts see all data - no filtering needed
            assigned_programs = None
            assigned_clients = None
        elif staff_profile.is_staff_only():
                is_staff_only = True
                # Get directly assigned clients for staff users
                from staff.models import StaffClientAssignment
                staff_assigned_clients = Client.objects.filter(
                    staff_assignments__staff=staff_profile,
                    staff_assignments__is_active=True
                ).distinct()
                # Get programs where assigned clients are enrolled
                assigned_programs = Program.objects.filter(
                    clientprogramenrollment__client__in=staff_assigned_clients
                ).distinct()
                # Get clients enrolled in assigned programs (same as staff_assigned_clients for staff users)
                assigned_clients = staff_assigned_clients
    except Exception:
        pass
    
    # Get basic statistics - filter for program managers and staff-only users
    today = timezone.now().date()
    user_can_see_archived_items = can_see_archived(request.user)
    
    # Calculate active clients count using the same logic as clients list view
    total_clients = get_active_clients_count(
        request,
        assigned_programs=assigned_programs,
        is_program_manager=is_program_manager,
        is_leader=is_leader,
        is_staff_only=is_staff_only,
        is_analyst=is_analyst
    )
    
    if is_program_manager and assigned_programs:
        active_programs_queryset = assigned_programs.filter(status='active')
        if not user_can_see_archived_items:
            active_programs_queryset = active_programs_queryset.filter(is_archived=False)
        active_programs = active_programs_queryset.count()
        total_staff = Staff.objects.count()  # Staff count is same for all
        
        # Get active restrictions count - Managers see ALL restrictions
        active_restrictions_queryset = ServiceRestriction.objects.filter(
            start_date__lte=today
        ).filter(
            Q(end_date__isnull=True) | Q(end_date__gte=today)
        )
        if not user_can_see_archived_items:
            active_restrictions_queryset = active_restrictions_queryset.filter(is_archived=False)
        active_restrictions = active_restrictions_queryset.count()
        
        # Get recent clients (last 5) from assigned programs
        recent_clients_queryset = Client.objects.filter(
            clientprogramenrollment__program__in=assigned_programs
        )
        if not user_can_see_archived_items:
            recent_clients_queryset = recent_clients_queryset.filter(is_archived=False)
        recent_clients = recent_clients_queryset.distinct().order_by('-created_at')[:5]
        
        # Get recent restrictions (last 5) - Managers see ALL restrictions
        recent_restrictions_queryset = ServiceRestriction.objects.all()
        if not user_can_see_archived_items:
            recent_restrictions_queryset = recent_restrictions_queryset.filter(is_archived=False)
        recent_restrictions = recent_restrictions_queryset.select_related('client', 'program').order_by('-created_at')[:5]
        
        # Get restricted clients (Agency-wide restrictions) - Managers see ALL agency-wide restrictions
        restricted_clients_queryset = ServiceRestriction.objects.filter(scope='org')
        if not user_can_see_archived_items:
            restricted_clients_queryset = restricted_clients_queryset.filter(is_archived=False)
        restricted_clients = restricted_clients_queryset.select_related('client', 'program').order_by('-created_at')[:10]
    elif is_leader and assigned_programs:
        active_programs_queryset = assigned_programs.filter(status='active')
        if not user_can_see_archived_items:
            active_programs_queryset = active_programs_queryset.filter(is_archived=False)
        active_programs = active_programs_queryset.count()
        total_staff = Staff.objects.count()  # Staff count is same for all
        
        # Get active restrictions count - Leaders see ALL restrictions
        active_restrictions_queryset = ServiceRestriction.objects.filter(
            start_date__lte=today
        ).filter(
            Q(end_date__isnull=True) | Q(end_date__gte=today)
        )
        if not user_can_see_archived_items:
            active_restrictions_queryset = active_restrictions_queryset.filter(is_archived=False)
        active_restrictions = active_restrictions_queryset.count()
        
        # Get recent clients (last 5) from assigned programs
        recent_clients_queryset = Client.objects.filter(
            clientprogramenrollment__program__in=assigned_programs
        )
        if not user_can_see_archived_items:
            recent_clients_queryset = recent_clients_queryset.filter(is_archived=False)
        recent_clients = recent_clients_queryset.distinct().order_by('-created_at')[:5]
        
        # Get recent restrictions (last 5) - Leaders see ALL restrictions
        recent_restrictions_queryset = ServiceRestriction.objects.all()
        if not user_can_see_archived_items:
            recent_restrictions_queryset = recent_restrictions_queryset.filter(is_archived=False)
        recent_restrictions = recent_restrictions_queryset.select_related('client', 'program').order_by('-created_at')[:5]
        
        # Get restricted clients (Agency-wide restrictions) - Leaders see ALL agency-wide restrictions
        restricted_clients_queryset = ServiceRestriction.objects.filter(scope='org')
        if not user_can_see_archived_items:
            restricted_clients_queryset = restricted_clients_queryset.filter(is_archived=False)
        restricted_clients = restricted_clients_queryset.select_related('client', 'program').order_by('-created_at')[:10]
    elif is_staff_only:
        active_programs_queryset = Program.objects.filter(status='active')
        if not user_can_see_archived_items:
            active_programs_queryset = active_programs_queryset.filter(is_archived=False)
        active_programs = active_programs_queryset.count()
        total_staff = Staff.objects.count()
        
        # Get active restrictions count - Staff users see ALL restrictions
        active_restrictions_queryset = ServiceRestriction.objects.filter(
            start_date__lte=today
        ).filter(
            Q(end_date__isnull=True) | Q(end_date__gte=today)
        )
        if not user_can_see_archived_items:
            active_restrictions_queryset = active_restrictions_queryset.filter(is_archived=False)
        active_restrictions = active_restrictions_queryset.count()
        
        # Get recent clients (last 5) - Staff users see ALL clients
        recent_clients_queryset = Client.objects.all()
        if not user_can_see_archived_items:
            recent_clients_queryset = recent_clients_queryset.filter(is_archived=False)
        recent_clients = recent_clients_queryset.order_by('-created_at')[:5]
        
        # Get recent restrictions (last 5) - Staff users see ALL restrictions
        recent_restrictions_queryset = ServiceRestriction.objects.all()
        if not user_can_see_archived_items:
            recent_restrictions_queryset = recent_restrictions_queryset.filter(is_archived=False)
        recent_restrictions = recent_restrictions_queryset.select_related('client', 'program').order_by('-created_at')[:5]
        
        # Get restricted clients (Agency-wide restrictions) - Staff users see ALL agency-wide restrictions
        restricted_clients_queryset = ServiceRestriction.objects.filter(scope='org')
        if not user_can_see_archived_items:
            restricted_clients_queryset = restricted_clients_queryset.filter(is_archived=False)
        restricted_clients = restricted_clients_queryset.select_related('client', 'program').order_by('-created_at')[:10]
    elif is_analyst:
        active_programs_queryset = Program.objects.filter(status='active')
        if not user_can_see_archived_items:
            active_programs_queryset = active_programs_queryset.filter(is_archived=False)
        active_programs = active_programs_queryset.count()
        total_staff = Staff.objects.count()
        
        # Get active restrictions count - Analysts see ALL restrictions
        active_restrictions_queryset = ServiceRestriction.objects.filter(
            start_date__lte=today
        ).filter(
            Q(end_date__isnull=True) | Q(end_date__gte=today)
        )
        if not user_can_see_archived_items:
            active_restrictions_queryset = active_restrictions_queryset.filter(is_archived=False)
        active_restrictions = active_restrictions_queryset.count()
        
        # Get recent clients (last 5) - Analysts see ALL clients
        recent_clients_queryset = Client.objects.all()
        if not user_can_see_archived_items:
            recent_clients_queryset = recent_clients_queryset.filter(is_archived=False)
        recent_clients = recent_clients_queryset.order_by('-created_at')[:5]
        
        # Get recent restrictions (last 5) - Analysts see ALL restrictions
        recent_restrictions_queryset = ServiceRestriction.objects.all()
        if not user_can_see_archived_items:
            recent_restrictions_queryset = recent_restrictions_queryset.filter(is_archived=False)
        recent_restrictions = recent_restrictions_queryset.select_related('client', 'program').order_by('-created_at')[:5]
        
        # Get restricted clients (Agency-wide restrictions) - Analysts see ALL agency-wide restrictions
        restricted_clients_queryset = ServiceRestriction.objects.filter(scope='org')
        if not user_can_see_archived_items:
            restricted_clients_queryset = restricted_clients_queryset.filter(is_archived=False)
        restricted_clients = restricted_clients_queryset.select_related('client', 'program').order_by('-created_at')[:10]
    else:
        active_programs_queryset = Program.objects.filter(status='active')
        if not user_can_see_archived_items:
            active_programs_queryset = active_programs_queryset.filter(is_archived=False)
        active_programs = active_programs_queryset.count()
        total_staff = Staff.objects.count()
        
        # Get active restrictions count
        active_restrictions = ServiceRestriction.objects.filter(
            is_archived=False,
            start_date__lte=today
        ).filter(
            Q(end_date__isnull=True) | Q(end_date__gte=today)
        ).count()
        
        # Get recent clients (last 5)
        recent_clients = Client.objects.order_by('-created_at')[:5]
        
        # Get recent restrictions (last 5)
        recent_restrictions = ServiceRestriction.objects.filter(
            is_archived=False
        ).select_related('client', 'program').order_by('-created_at')[:5]
        
        # Get restricted clients (Agency-wide restrictions)
        restricted_clients = ServiceRestriction.objects.filter(
            is_archived=False,
            scope='org'
        ).select_related('client', 'program').order_by('-created_at')[:10]
    
    # Get program status with enrollment counts and capacity information
    # Filter programs based on user role
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            if staff.is_program_manager():
                # Manager can only see their assigned programs
                programs = staff.get_assigned_programs()
            elif is_staff_only:
                # Staff-only users see only programs where their assigned clients are enrolled
                programs = assigned_programs if assigned_programs else Program.objects.none()
            else:
                # SuperAdmin and other roles can see all programs
                programs = Program.objects.all()
        except Exception:
            programs = Program.objects.none()
    else:
        programs = Program.objects.none()
    
    program_status = []
    
    # Limit to first 5 programs
    programs_limited = programs[:5]
    
    for program in programs_limited:
        current_enrollments = program.get_current_enrollments_count()
        available_capacity = program.get_available_capacity()
        capacity_percentage = program.get_capacity_percentage()
        is_at_capacity = program.is_at_capacity()
        
        program_status.append({
            'name': program.name,
            'department': program.department,
            'capacity_current': program.capacity_current,
            'current_enrollments': current_enrollments,
            'available_capacity': available_capacity,
            'capacity_percentage': round(capacity_percentage, 1),
            'is_at_capacity': is_at_capacity
        })
    
    context = {
        'total_clients': total_clients,
        'active_programs': active_programs,
        'total_staff': total_staff,
        'active_restrictions': active_restrictions,
        'recent_clients': recent_clients,
        'recent_restrictions': recent_restrictions,
        'restricted_clients': restricted_clients,
        'program_status': program_status,
        'is_program_manager': is_program_manager,
        'is_staff_only': is_staff_only,
        'assigned_programs': assigned_programs,
        'staff_assigned_clients': staff_assigned_clients,
    }
    
    return render(request, 'dashboard.html', context)


@jwt_required
def departments(request):
    """Departments management view"""
    # Check if user is Analyst and block access
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            # Block Analyst users from accessing departments page
            if 'Analyst' in role_names and not any(role in ['SuperAdmin', 'Manager', 'Leader', 'Staff'] for role in role_names):
                messages.error(request, 'Analyst users can only access Dashboard and Reports. Individual pages are not accessible.')
                return redirect('dashboard')
        except Exception:
            pass
    
    # Get departments - filter for Managers and Leaders
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            if staff.is_program_manager():
                # Manager can only see departments of their assigned programs
                # Get assigned departments and add proper annotations
                assigned_departments = staff.get_assigned_departments()
                assigned_programs = staff.get_assigned_programs().filter(is_archived=False)
                departments = Department.objects.filter(
                    id__in=assigned_departments.values_list('id', flat=True),
                    is_archived=False
                ).exclude(
                    name__iexact='HASS'
                ).annotate(
                    program_count=Count('program', filter=Q(program__in=assigned_programs, program__is_archived=False), distinct=True),
                    staff_count=Count('program__programstaff__staff', filter=Q(program__programstaff__program__is_archived=False), distinct=True)
                ).order_by('name')
            elif staff.is_leader():
                # Leader can only see their assigned departments
                assigned_departments = Department.objects.filter(
                    leader_assignments__staff=staff,
                    leader_assignments__is_active=True,
                    is_archived=False
                ).distinct()
                assigned_programs = Program.objects.filter(
                    department__in=assigned_departments,
                    is_archived=False
                ).distinct()
                departments = assigned_departments.exclude(
                    name__iexact='HASS'
                ).annotate(
                    program_count=Count('program', filter=Q(program__in=assigned_programs, program__is_archived=False), distinct=True),
                    staff_count=Count('program__programstaff__staff', filter=Q(program__programstaff__program__is_archived=False), distinct=True)
                ).order_by('name')
            else:
                # SuperAdmin and Staff can see all departments (excluding HASS)
                departments = Department.objects.filter(
                    is_archived=False
                ).exclude(
                    name__iexact='HASS'
                ).annotate(
                    program_count=Count('program', filter=Q(program__is_archived=False), distinct=True),
                    staff_count=Count('program__programstaff__staff', filter=Q(program__programstaff__program__is_archived=False), distinct=True)
                ).order_by('name')
        except Exception:
            departments = Department.objects.none()
    else:
        departments = Department.objects.none()
    
    # Get statistics based on filtered departments
    total_departments = departments.count()
    
    # Calculate total programs based on filtered departments
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            if staff.is_program_manager():
                # Manager sees only programs they are assigned to manage
                total_programs = staff.get_assigned_programs().filter(is_archived=False).count()
            elif staff.is_leader():
                # Leader sees only programs from their assigned departments
                assigned_departments = Department.objects.filter(
                    leader_assignments__staff=staff,
                    leader_assignments__is_active=True,
                    is_archived=False
                ).distinct()
                total_programs = Program.objects.filter(
                    department__in=assigned_departments,
                    is_archived=False
                ).distinct().count()
            else:
                # SuperAdmin and Staff see all programs
                total_programs = Program.objects.filter(is_archived=False).count()
        except Exception:
            total_programs = Program.objects.filter(is_archived=False).count()
    else:
        total_programs = Program.objects.filter(is_archived=False).count()
    
    # Calculate total staff based on user role
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            if staff.is_program_manager():
                # Manager sees only staff from their assigned programs
                total_staff = Staff.objects.filter(
                    program_assignments__program__in=staff.get_assigned_programs(),
                    program_assignments__is_active=True
                ).distinct().count()
            elif staff.is_leader():
                # Leader sees only staff from their assigned departments
                assigned_departments = Department.objects.filter(
                    leader_assignments__staff=staff,
                    leader_assignments__is_active=True,
                    is_archived=False
                ).distinct()
                assigned_programs = Program.objects.filter(
                    department__in=assigned_departments
                ).distinct()
                total_staff = Staff.objects.filter(
                    program_assignments__program__in=assigned_programs,
                    program_assignments__is_active=True
                ).distinct().count()
            else:
                # SuperAdmin and Staff see all staff
                total_staff = Staff.objects.count()
        except Exception:
            total_staff = Staff.objects.count()
    else:
        total_staff = Staff.objects.count()
    
    # Check if user is a Leader
    is_leader = False
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            is_leader = staff.is_leader()
        except Exception:
            pass
    
    context = {
        'departments': departments,
        'total_departments': total_departments,
        'total_programs': total_programs,
        'total_staff': total_staff,
        'is_leader': is_leader,
    }
    
    return render(request, 'core/departments.html', context)


@jwt_required
def enrollments(request):
    """Enrollments management view"""
    # Check if user is Analyst and block access
    if request.user.is_authenticated:
        try:
            staff = request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            # Block Analyst users from accessing enrollments page
            if 'Analyst' in role_names and not any(role in ['SuperAdmin', 'Manager', 'Leader', 'Staff'] for role in role_names):
                messages.error(request, 'Analyst users can only access Dashboard and Reports. Individual pages are not accessible.')
                return redirect('dashboard')
        except Exception:
            pass
    
    # Get all enrollments with related data
    enrollments = ClientProgramEnrollment.objects.select_related(
        'client', 'program', 'program__department'
    ).order_by('-start_date')
    
    # Calculate statistics using date-based logic
    from django.utils import timezone
    today = timezone.now().date()
    
    total_enrollments = enrollments.count()
    active_count = 0
    completed_count = 0
    pending_count = 0
    
    for enrollment in enrollments:
        if today < enrollment.start_date:
            pending_count += 1
        elif enrollment.end_date:
            if today > enrollment.end_date:
                completed_count += 1
            else:
                active_count += 1
        else:
            active_count += 1
    
    # Calculate success rate
    success_rate = 0
    if total_enrollments > 0:
        success_rate = round((completed_count / total_enrollments) * 100, 1)
    
    context = {
        'enrollments': enrollments,
        'total_enrollments': total_enrollments,
        'active_enrollments': active_count,
        'completed_enrollments': completed_count,
        'pending_enrollments': pending_count,
        'success_rate': success_rate,
    }
    
    return render(request, 'core/enrollments.html', context)


@method_decorator(jwt_required, name='dispatch')
class RestrictionListView(AnalystAccessMixin, ProgramManagerAccessMixin, ListView):
    model = ServiceRestriction
    template_name = 'core/restrictions.html'
    context_object_name = 'restrictions'
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
    
    def get_accessible_programs(self):
        """Get all programs that the current user has access to based on their roles"""
        if not self.request.user or not self.request.user.is_authenticated:
            return Program.objects.filter(status='active', is_archived=False).order_by('name')
        
        try:
            staff = self.request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            # SuperAdmin and Admin see all programs
            if any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                return Program.objects.filter(status='active', is_archived=False).order_by('name')
            
            # Analyst sees all programs (for reporting purposes)
            elif 'Analyst' in role_names and not any(role in ['SuperAdmin', 'Manager', 'Leader', 'Staff'] for role in role_names):
                return Program.objects.filter(status='active', is_archived=False).order_by('name')
            
            # Manager sees programs they're assigned to
            elif staff.is_program_manager():
                return staff.get_assigned_programs().filter(status='active', is_archived=False).order_by('name')
            
            # Leader sees programs in departments they lead
            elif staff.is_leader():
                assigned_departments = staff.get_assigned_departments()
                return Program.objects.filter(
                    department__in=assigned_departments,
                    status='active',
                    is_archived=False
                ).distinct().order_by('name')
            
            # Staff sees programs where their assigned clients are enrolled
            elif 'Staff' in role_names:
                from staff.models import StaffClientAssignment
                assigned_client_ids = StaffClientAssignment.objects.filter(
                    staff=staff,
                    is_active=True
                ).values_list('client_id', flat=True)
                return Program.objects.filter(
                    clientprogramenrollment__client_id__in=assigned_client_ids,
                    status='active',
                    is_archived=False
                ).distinct().order_by('name')
            
            # Default: show all active programs
            else:
                return Program.objects.filter(status='active', is_archived=False).order_by('name')
                
        except Exception:
            # If there's any error, show all active programs
            return Program.objects.filter(status='active', is_archived=False).order_by('name')
    
    def get_queryset(self):
        # First apply the ProgramManagerAccessMixin filtering
        queryset = super().get_queryset()
        
        # Check if user can see archived restrictions (only SuperAdmin and Admin)
        user_can_see_archived_items = can_see_archived(self.request.user)
        
        # Only show approved restrictions to non-admin users (SuperAdmin/Admin need to see pending ones to approve them)
        if not user_can_see_archived_items:
            queryset = queryset.filter(is_approved=True)
        
        # Exclude archived restrictions for non-admin users unless explicitly requested
        status_filter = self.request.GET.get('status', '')
        if not user_can_see_archived_items:
            # Non-admin users cannot see archived restrictions at all
            queryset = queryset.filter(is_archived=False)
        elif status_filter != 'archived':
            # Admin users: exclude archived unless status filter is 'archived'
            queryset = queryset.filter(is_archived=False)
        
        # Apply date range filtering
        start_date, end_date, parsed_start_date, parsed_end_date = get_date_range_filter(self.request)
        
        if parsed_start_date:
            queryset = queryset.filter(start_date__gte=parsed_start_date)
        if parsed_end_date:
            # Include restrictions that either:
            # 1. Have an end date on or before the end date, OR
            # 2. Don't have an end date (NULL) but their start date is on or before the end date
            from django.db.models import Q
            queryset = queryset.filter(
                Q(end_date__lte=parsed_end_date) | 
                Q(end_date__isnull=True, start_date__lte=parsed_end_date)
            )
        
        # Apply additional filters
        restriction_type_filter = self.request.GET.get('restriction_type', '')
        scope_filter = self.request.GET.get('scope', '')
        status_filter = self.request.GET.get('status', '')
        bill_168_filter = self.request.GET.get('bill_168', '')
        no_trespass_filter = self.request.GET.get('no_trespass', '')
        search_query = self.request.GET.get('search', '').strip()
        
        if restriction_type_filter:
            queryset = queryset.filter(restriction_type=restriction_type_filter)
        
        if scope_filter:
            queryset = queryset.filter(scope=scope_filter)
        
        if bill_168_filter:
            if bill_168_filter == 'true':
                queryset = queryset.filter(is_bill_168=True)
            elif bill_168_filter == 'false':
                queryset = queryset.filter(is_bill_168=False)
        
        if no_trespass_filter:
            if no_trespass_filter == 'true':
                queryset = queryset.filter(is_no_trespass=True)
            elif no_trespass_filter == 'false':
                queryset = queryset.filter(is_no_trespass=False)
        
        if status_filter and status_filter != 'all':
            if status_filter == 'active':
                # Use the model's is_active method logic
                today = timezone.now().date()
                queryset = queryset.filter(
                    is_archived=False,
                    start_date__lte=today
                ).filter(
                    Q(is_indefinite=True) | 
                    Q(end_date__isnull=True) | 
                    Q(end_date__gte=today)
                )
            elif status_filter == 'expired':
                # Use the model's is_expired method logic
                today = timezone.now().date()
                queryset = queryset.filter(
                    is_archived=False,
                    is_indefinite=False,
                    end_date__isnull=False,
                    end_date__lt=today
                )
            elif status_filter == 'archived':
                # Only SuperAdmin/Admin can see archived restrictions
                if user_can_see_archived_items:
                    queryset = queryset.filter(is_archived=True)
                else:
                    # Non-admin users cannot filter for archived - show empty
                    queryset = queryset.none()
        
        if search_query:
            queryset = queryset.filter(
                Q(client__first_name__icontains=search_query) |
                Q(client__last_name__icontains=search_query) |
                Q(notes__icontains=search_query)
            )
        
        # Apply client search
        client_search_query = self.request.GET.get('client_search', '').strip()
        if client_search_query:
            queryset = queryset.filter(
                Q(client__first_name__icontains=client_search_query) |
                Q(client__last_name__icontains=client_search_query)
            )
        
        # Apply program filter (by ID instead of search)
        program_filter = self.request.GET.get('program', '').strip()
        client_sort = self.request.GET.get('client_sort', '').strip()
        if program_filter:
            try:
                program_id = int(program_filter)
                queryset = queryset.filter(program_id=program_id)
            except (ValueError, TypeError):
                pass
        
        # Apply client sorting with case-insensitive ordering (first name, then last name)
        from django.db.models.functions import Lower
        if client_sort == 'name_asc':
            queryset = queryset.annotate(
                client_first_name_lower=Lower('client__first_name'),
                client_last_name_lower=Lower('client__last_name')
            ).order_by('client_first_name_lower', 'client_last_name_lower', '-start_date')
        elif client_sort == 'name_desc':
            queryset = queryset.annotate(
                client_first_name_lower=Lower('client__first_name'),
                client_last_name_lower=Lower('client__last_name')
            ).order_by('-client_first_name_lower', '-client_last_name_lower', '-start_date')
        else:
            queryset = queryset.order_by('-start_date')
        
        return queryset.select_related('client', 'program', 'program__department')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        print(f"DEBUG: Context keys after super(): {list(context.keys())}")
        if 'restrictions' in context:
            print(f"DEBUG: Restrictions in context: {len(context['restrictions'])}")
        else:
            print("DEBUG: No 'restrictions' key in context!")
        
        # Get the total count of filtered restrictions (not just current page)
        total_filtered_count = self.get_queryset().count()
        
        # Get filtered restrictions for statistics (not paginated)
        # Use the same base queryset as the main queryset but without pagination
        base_queryset = super().get_queryset()
        
        # Apply the same filters as the main queryset but without search filters
        restriction_type_filter = self.request.GET.get('restriction_type', '')
        scope_filter = self.request.GET.get('scope', '')
        status_filter = self.request.GET.get('status', '')
        bill_168_filter = self.request.GET.get('bill_168', '')
        no_trespass_filter = self.request.GET.get('no_trespass', '')
        
        if restriction_type_filter:
            base_queryset = base_queryset.filter(restriction_type=restriction_type_filter)
        
        if scope_filter:
            base_queryset = base_queryset.filter(scope=scope_filter)
        
        if bill_168_filter:
            if bill_168_filter == 'true':
                base_queryset = base_queryset.filter(is_bill_168=True)
            elif bill_168_filter == 'false':
                base_queryset = base_queryset.filter(is_bill_168=False)
        
        if no_trespass_filter:
            if no_trespass_filter == 'true':
                base_queryset = base_queryset.filter(is_no_trespass=True)
            elif no_trespass_filter == 'false':
                base_queryset = base_queryset.filter(is_no_trespass=False)
        
        if status_filter:
            if status_filter == 'active':
                base_queryset = base_queryset.filter(end_date__isnull=True) | base_queryset.filter(end_date__gt=timezone.now().date())
            elif status_filter == 'expired':
                base_queryset = base_queryset.filter(end_date__lt=timezone.now().date())
        
        # Get all filtered restrictions for statistics
        all_restrictions = base_queryset.select_related(
            'client', 'program', 'program__department'
        )
        
        # Calculate statistics
        context['total_restrictions'] = all_restrictions.count()
        
        # Calculate active restrictions using the model's is_active method
        active_count = 0
        expired_count = 0
        archived_count = 0
        
        for restriction in all_restrictions:
            if restriction.is_archived:
                archived_count += 1
            elif restriction.is_expired():
                expired_count += 1
            elif restriction.is_active():
                active_count += 1
        
        context['active_restrictions'] = active_count
        context['expired_restrictions'] = expired_count
        context['archived_restrictions'] = archived_count
        context['org_restrictions'] = all_restrictions.filter(scope='org').count()
        context['program_restrictions'] = all_restrictions.filter(scope='program').count()
        context['total_filtered_count'] = total_filtered_count
        from django.utils import timezone as django_timezone
        context['today'] = django_timezone.now().date()
        
        # Add filter options to context
        context['restriction_type_choices'] = [
            ('', 'All Types'),
            ('bill_168', 'Bill 168 (Violence Against Staff)'),
            ('no_trespass', 'No Trespass Order'),
            ('behaviors', 'Behavioral Restrictions'),
        ]
        
        context['scope_choices'] = [
            ('', 'All Scopes'),
            ('org', 'Agency-wide'),
            ('program', 'Program-Specific'),
        ]
        
        context['status_choices'] = [
            ('all', 'All'),
            ('active', 'Active'),
            ('expired', 'Expired'),
            ('archived', 'Archived'),
        ]
        
        # Add current filter values
        context['current_restriction_type'] = self.request.GET.get('restriction_type', '')
        context['current_scope'] = self.request.GET.get('scope', '')
        context['current_status'] = self.request.GET.get('status', 'all')
        context['current_bill_168'] = self.request.GET.get('bill_168', '')
        context['current_no_trespass'] = self.request.GET.get('no_trespass', '')
        context['current_search'] = self.request.GET.get('search', '')
        context['per_page'] = self.request.GET.get('per_page', '10')
        
        # Get accessible programs for dropdown
        context['programs'] = self.get_accessible_programs()
        
        # Add search parameters
        context['client_search'] = self.request.GET.get('client_search', '')
        context['current_program'] = self.request.GET.get('program', '')
        context['client_sort'] = self.request.GET.get('client_sort', '')
        
        # Add date filter parameters
        start_date, end_date, parsed_start_date, parsed_end_date = get_date_range_filter(self.request)
        context['start_date'] = start_date
        context['end_date'] = end_date
        
        # Override pagination context
        context['is_paginated'] = True  # Always show pagination controls
        context['per_page'] = str(self.get_paginate_by(self.get_queryset()))
        
        return context




@method_decorator(jwt_required, name='dispatch')
class AuditLogListView(ListView):
    """Audit log list view with pagination - SuperAdmin only"""
    model = AuditLog
    template_name = 'core/audit_log.html'
    context_object_name = 'audit_logs'
    paginate_by = 10  # Default: Show 10 audit logs per page
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to view audit logs"""
        # Only SuperAdmin can view audit logs
        if not request.user.is_superuser:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    messages.error(request, 'You do not have permission to view audit logs.')
                    return redirect('dashboard')
            except:
                messages.error(request, 'You do not have permission to view audit logs.')
                return redirect('dashboard')
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_paginate_by(self, queryset):
        """Get number of items to paginate by from request parameter"""
        per_page = self.request.GET.get('per_page', self.paginate_by)
        try:
            return int(per_page)
        except (ValueError, TypeError):
            return self.paginate_by
    
    def get_queryset(self):
        """Get audit logs with filtering by date range and entity"""
        queryset = AuditLog.objects.select_related('changed_by')
        
        # Apply date range filtering
        start_date = self.request.GET.get('start_date', '').strip()
        end_date = self.request.GET.get('end_date', '').strip()
        
        if start_date:
            try:
                parsed_start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                # Convert to datetime for comparison with changed_at (DateTimeField)
                start_datetime = timezone.make_aware(datetime.combine(parsed_start_date, datetime.min.time()))
                queryset = queryset.filter(changed_at__gte=start_datetime)
            except ValueError:
                pass
        
        if end_date:
            try:
                parsed_end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                # Convert to datetime for comparison with changed_at (DateTimeField)
                end_datetime = timezone.make_aware(datetime.combine(parsed_end_date, datetime.max.time()))
                queryset = queryset.filter(changed_at__lte=end_datetime)
            except ValueError:
                pass
        
        # Apply entity filter
        entity_filter = self.request.GET.get('entity', '').strip()
        if entity_filter:
            queryset = queryset.filter(entity=entity_filter)
        
        return queryset.order_by('-changed_at')
    
    def get_context_data(self, **kwargs):
        """Add statistics to context"""
        context = super().get_context_data(**kwargs)
        
        # Get filtered queryset for statistics
        filtered_queryset = self.get_queryset()
        all_audit_logs = AuditLog.objects.all()
        
        # Statistics for filtered results
        context['total_events'] = filtered_queryset.count()
        context['create_events'] = filtered_queryset.filter(action='create').count()
        context['update_events'] = filtered_queryset.filter(action='update').count()
        context['delete_events'] = filtered_queryset.filter(action='delete').count()
        
        # Get all unique entities for filter dropdown
        context['all_entities'] = sorted(all_audit_logs.values_list('entity', flat=True).distinct())
        
        # Add filter values to context
        context['start_date'] = self.request.GET.get('start_date', '')
        context['end_date'] = self.request.GET.get('end_date', '')
        context['current_entity'] = self.request.GET.get('entity', '')
        
        # Add per_page to context for the template
        context['per_page'] = str(self.get_paginate_by(self.get_queryset()))
        
        return context


@csrf_exempt
@require_http_methods(["POST"])
@jwt_required
def clear_old_audit_logs(request):
    """Clear audit log entries older than 7 days - SuperAdmin/Admin only"""
    # Check if user has permission
    if not request.user.is_authenticated:
        return JsonResponse({
            'success': False,
            'error': 'Authentication required'
        }, status=401)
    
    # Only SuperAdmin or Admin can clear audit logs
    if not request.user.is_superuser:
        try:
            staff = request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                return JsonResponse({
                    'success': False,
                    'error': 'You do not have permission to clear audit logs. Only SuperAdmin and Admin users can perform this action.'
                }, status=403)
        except Exception:
            return JsonResponse({
                'success': False,
                'error': 'You do not have permission to clear audit logs.'
            }, status=403)
    
    try:
        from django.utils import timezone
        from datetime import timedelta
        
        # Calculate the cutoff date (7 days ago)
        cutoff_date = timezone.now() - timedelta(days=7)
        
        # Get count of entries to be deleted
        old_logs = AuditLog.objects.filter(changed_at__lt=cutoff_date)
        deleted_count = old_logs.count()
        
        if deleted_count == 0:
            return JsonResponse({
                'success': True,
                'deleted_count': 0,
                'message': 'No audit log entries older than 7 days found.'
            })
        
        # Delete the entries
        old_logs.delete()
        
        # Create audit log for this action
        from .models import create_audit_log
        try:
            create_audit_log(
                entity_name='AuditLog',
                entity_id=uuid.uuid4(),
                action='delete',
                changed_by=request.user,
                diff_data={
                    'deleted_count': deleted_count,
                    'cutoff_date': cutoff_date.isoformat(),
                    'cleared_by': request.user.get_full_name() or request.user.username
                }
            )
        except Exception as e:
            logging.getLogger(__name__).error(f"Error creating audit log for clearing old logs: {e}")
        
        return JsonResponse({
            'success': True,
            'deleted_count': deleted_count,
            'message': f'Successfully cleared {deleted_count} audit log entr{"y" if deleted_count == 1 else "ies"} older than 7 days.'
        })
        
    except Exception as e:
        logger.error(f"Error clearing old audit logs: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': f'An error occurred while clearing audit logs: {str(e)}'
        }, status=500)


@method_decorator(jwt_required, name='dispatch')
class AuditLogRestoreView(View):
    """Restore a deleted record from audit log"""
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to restore records"""
        # Only SuperAdmin and Admin can restore records
        if not request.user.is_superuser:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    return JsonResponse({'success': False, 'error': 'You do not have permission to restore records.'}, status=403)
            except:
                return JsonResponse({'success': False, 'error': 'You do not have permission to restore records.'}, status=403)
        
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request, log_id):
        """Restore a deleted record"""
        import json
        from django.db import transaction
        from django.http import JsonResponse
        
        try:
            # Get the audit log entry - handle both 'delete' and 'archive' actions
            try:
                audit_log = AuditLog.objects.get(id=log_id, action__in=['delete', 'archive'])
            except AuditLog.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Audit log entry not found or not restorable.'}, status=404)
            
            # Get the diff data from request or audit log
            if request.content_type == 'application/json':
                data = json.loads(request.body)
                diff_data = data.get('diff_data', audit_log.diff_json)
                entity = data.get('entity', audit_log.entity)
                entity_id = data.get('entity_id', audit_log.entity_id)
            else:
                diff_data = audit_log.diff_json
                entity = audit_log.entity
                entity_id = audit_log.entity_id
            
            if not diff_data:
                return JsonResponse({'success': False, 'error': 'No data available to restore.'}, status=400)
            
            # Restore based on entity type
            with transaction.atomic():
                if entity == 'Client':
                    restored = self._restore_client(entity_id, diff_data, request.user)
                elif entity == 'Program':
                    restored = self._restore_program(entity_id, diff_data, request.user)
                elif entity == 'Enrollment':
                    restored = self._restore_enrollment(entity_id, diff_data, request.user)
                elif entity == 'Restriction':
                    restored = self._restore_restriction(entity_id, diff_data, request.user)
                elif entity == 'Department':
                    restored = self._restore_department(entity_id, diff_data, request.user)
                else:
                    return JsonResponse({'success': False, 'error': f'Entity type "{entity}" is not supported for restoration.'}, status=400)
                
                if restored:
                    # Get a descriptive name for the restored item
                    restore_name = None
                    if hasattr(restored, '_restore_name'):
                        restore_name = restored._restore_name
                    elif entity == 'Client':
                        restore_name = f"{restored.first_name} {restored.last_name}"
                    elif entity == 'Program':
                        restore_name = f"{restored.name} ({restored.department.name})"
                    elif entity == 'Enrollment':
                        restore_name = f"{restored.client.first_name} {restored.client.last_name} - {restored.program.name}"
                    elif entity == 'Restriction':
                        restore_name = f"{restored.client.first_name} {restored.client.last_name} - {restored.get_restriction_type_display()}"
                    elif entity == 'Department':
                        restore_name = restored.name
                    
                    # Create audit log for restoration
                    from .models import create_audit_log
                    create_audit_log(
                        entity_name=entity,
                        entity_id=restored.external_id,
                        action='restore',
                        changed_by=request.user,
                        diff_data={
                            'restored_from_audit_log': str(audit_log.id),
                            'restored_by': request.user.get_full_name() or request.user.username,
                            'restored_item': restore_name or str(restored)
                        }
                    )
                    
                    # Delete the original archive/delete audit log entry since the item has been restored
                    audit_log.delete()
                    
                    # Create a clear success message
                    if restore_name:
                        success_message = f'{entity} "{restore_name}" has been restored successfully.'
                    else:
                        success_message = f'{entity} restored successfully.'
                    
                    return JsonResponse({
                        'success': True,
                        'message': success_message,
                        'entity_id': str(restored.external_id),
                        'restored_item': restore_name
                    })
                else:
                    return JsonResponse({'success': False, 'error': 'Failed to restore record.'}, status=500)
                    
        except AuditLog.DoesNotExist:
            return JsonResponse({'success': False, 'error': 'Audit log entry not found.'}, status=404)
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error restoring record: {e}")
            return JsonResponse({'success': False, 'error': f'An error occurred: {str(e)}'}, status=500)
    
    def _restore_client(self, entity_id, diff_data, user):
        """Restore a deleted/archived client"""
        from core.models import Client
        
        # Check if client already exists (including archived)
        try:
            existing = Client.objects.get(external_id=entity_id)
            # If archived, restore it
            if existing.is_archived:
                existing.is_archived = False
                existing.archived_at = None
                existing.updated_by = user.get_full_name() or user.username
                existing.save()
            return existing  # Already exists (restored or active)
        except Client.DoesNotExist:
            pass
        
        # Create new client with stored data
        client = Client.objects.create(
            external_id=entity_id,
            first_name=diff_data.get('first_name', ''),
            last_name=diff_data.get('last_name', ''),
            preferred_name=diff_data.get('preferred_name') or '',
            alias=diff_data.get('alias') or '',
            dob=diff_data.get('dob') if diff_data.get('dob') else None,
            gender=diff_data.get('gender') or '',
            sexual_orientation=diff_data.get('sexual_orientation') or '',
            citizenship_status=diff_data.get('citizenship_status') or '',
            indigenous_status=diff_data.get('indigenous_status') or '',
            country_of_birth=diff_data.get('country_of_birth') or '',
            address_2=diff_data.get('address_2') or '',
            permission_to_email=diff_data.get('permission_to_email', False),
            permission_to_phone=diff_data.get('permission_to_phone', False),
            phone_work=diff_data.get('phone_work') or '',
            phone_alt=diff_data.get('phone_alt') or '',
            client_id=diff_data.get('client_id') or '',
            medical_conditions=diff_data.get('medical_conditions') or '',
            primary_diagnosis=diff_data.get('primary_diagnosis') or '',
            comments=diff_data.get('comments') or '',
        )
        
        # Restore JSON fields if they exist
        if diff_data.get('languages_spoken'):
            try:
                import json
                client.languages_spoken = json.loads(diff_data['languages_spoken']) if isinstance(diff_data['languages_spoken'], str) else diff_data['languages_spoken']
            except:
                pass
        
        if diff_data.get('ethnicity'):
            try:
                import json
                client.ethnicity = json.loads(diff_data['ethnicity']) if isinstance(diff_data['ethnicity'], str) else diff_data['ethnicity']
            except:
                pass
        
        if diff_data.get('support_workers'):
            try:
                import json
                client.support_workers = json.loads(diff_data['support_workers']) if isinstance(diff_data['support_workers'], str) else diff_data['support_workers']
            except:
                pass
        
        if diff_data.get('next_of_kin'):
            try:
                import json
                client.next_of_kin = json.loads(diff_data['next_of_kin']) if isinstance(diff_data['next_of_kin'], str) else diff_data['next_of_kin']
            except:
                pass
        
        if diff_data.get('emergency_contact'):
            try:
                import json
                client.emergency_contact = json.loads(diff_data['emergency_contact']) if isinstance(diff_data['emergency_contact'], str) else diff_data['emergency_contact']
            except:
                pass
        
        client.save()
        return client
    
    def _restore_program(self, entity_id, diff_data, user):
        """Restore a deleted/archived program"""
        from core.models import Program, Department
        
        # Check if program already exists (including archived)
        try:
            existing = Program.objects.get(external_id=entity_id)
            # If archived, restore it
            if existing.is_archived:
                existing.is_archived = False
                existing.archived_at = None
                existing.updated_by = user.get_full_name() or user.username
                existing.save()
            return existing  # Already exists (restored or active)
        except Program.DoesNotExist:
            pass
        
        # Find department by name (exclude archived for audit log display)
        department = None
        if diff_data.get('department'):
            try:
                department = Department.objects.filter(
                    name__iexact=diff_data['department'],
                    is_archived=False
                ).first()
            except:
                pass
        
        # Create new program
        program = Program.objects.create(
            external_id=entity_id,
            name=diff_data.get('name', 'Restored Program'),
            department=department,
            location=diff_data.get('location') or '',
            capacity_current=diff_data.get('capacity_current', 0),
            capacity_effective_date=diff_data.get('capacity_effective_date') if diff_data.get('capacity_effective_date') else None,
            status='active'
        )
        
        return program
    
    def _restore_enrollment(self, entity_id, diff_data, user):
        """Restore a deleted/archived enrollment"""
        from core.models import ClientProgramEnrollment, Client, Program
        import uuid
        
        # Convert entity_id to UUID if it's a string
        try:
            if isinstance(entity_id, str):
                entity_id = uuid.UUID(entity_id)
        except (ValueError, TypeError):
            pass
        
        # Check if enrollment already exists (including archived) by external_id
        try:
            existing = ClientProgramEnrollment.objects.get(external_id=entity_id)
            enrollment_name = f"{existing.client.first_name} {existing.client.last_name} - {existing.program.name}"
            
            # If archived, restore it
            if existing.is_archived:
                existing.is_archived = False
                existing.archived_at = None
                existing.updated_by = user.get_full_name() or user.username
                existing.save()
                # Store enrollment name for success message
                existing._restore_name = enrollment_name
            return existing  # Already exists (restored or active)
        except ClientProgramEnrollment.DoesNotExist:
            pass
        except (ValueError, TypeError):
            # entity_id is not a valid UUID, try to find by other means
            pass
        
        # If enrollment doesn't exist, try to recreate from diff_data
        # Find client and program
        client = None
        program = None
        
        if diff_data.get('client'):
            # Try to find client by name
            try:
                client_name = diff_data['client']
                if isinstance(client_name, str):
                    name_parts = client_name.split()
                    if len(name_parts) >= 2:
                        client = Client.objects.filter(
                            first_name__iexact=name_parts[0], 
                            last_name__iexact=name_parts[-1]
                        ).first()
                    elif len(name_parts) == 1:
                        # Try last name only
                        client = Client.objects.filter(last_name__iexact=name_parts[0]).first()
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error finding client for enrollment restore: {e}")
        
        if diff_data.get('program'):
            try:
                program_name = diff_data['program']
                if isinstance(program_name, str):
                    program = Program.objects.filter(name__iexact=program_name).first()
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error finding program for enrollment restore: {e}")
        
        if not client or not program:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Cannot restore enrollment: client={client}, program={program}, diff_data={diff_data}")
            return None  # Cannot restore without client and program
        
        # Create enrollment
        from datetime import datetime
        try:
            enrollment = ClientProgramEnrollment.objects.create(
                external_id=entity_id,
                client=client,
                program=program,
                start_date=datetime.strptime(diff_data['start_date'], '%Y-%m-%d').date() if diff_data.get('start_date') else None,
                end_date=datetime.strptime(diff_data['end_date'], '%Y-%m-%d').date() if diff_data.get('end_date') else None,
                status=diff_data.get('status', 'pending'),
                created_by=user.get_full_name() or user.username
            )
            enrollment_name = f"{client.first_name} {client.last_name} - {program.name}"
            enrollment._restore_name = enrollment_name
            return enrollment
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error creating enrollment during restore: {e}")
            return None
    
    def _restore_restriction(self, entity_id, diff_data, user):
        """Restore a deleted/archived restriction"""
        from core.models import ServiceRestriction, Client
        
        # Check if restriction already exists (including archived)
        try:
            existing = ServiceRestriction.objects.get(external_id=entity_id)
            # If archived, restore it
            if existing.is_archived:
                existing.is_archived = False
                existing.archived_at = None
                existing.updated_by = user.get_full_name() or user.username
                existing.save()
            return existing  # Already exists (restored or active)
        except ServiceRestriction.DoesNotExist:
            pass
        
        # Find client
        client = None
        if diff_data.get('client'):
            try:
                name_parts = diff_data['client'].split()
                if len(name_parts) >= 2:
                    client = Client.objects.filter(first_name__iexact=name_parts[0], last_name__iexact=name_parts[-1]).first()
            except:
                pass
        
        if not client:
            return None
        
        # Create restriction
        from datetime import datetime
        restriction = ServiceRestriction.objects.create(
            external_id=entity_id,
            client=client,
            restriction_type=diff_data.get('restriction_type', ''),
            start_date=datetime.strptime(diff_data['start_date'], '%Y-%m-%d').date() if diff_data.get('start_date') else None,
            end_date=datetime.strptime(diff_data['end_date'], '%Y-%m-%d').date() if diff_data.get('end_date') else None,
            notes=diff_data.get('notes', '')
        )
        
        return restriction
    
    def _restore_department(self, entity_id, diff_data, user):
        """Restore a deleted/archived department"""
        from core.models import Department
        
        # Check if department already exists (including archived)
        try:
            existing = Department.objects.get(external_id=entity_id)
            # If archived, restore it
            if existing.is_archived:
                existing.is_archived = False
                existing.archived_at = None
                existing.save()
            return existing  # Already exists (restored or active)
        except Department.DoesNotExist:
            pass
        
        # Create new department with stored data
        department = Department.objects.create(
            external_id=entity_id,
            name=diff_data.get('name', '')
        )
        
        return department


# Department CRUD Views
class DepartmentListView(AnalystAccessMixin, ListView):
    model = Department
    template_name = 'core/departments.html'
    context_object_name = 'departments'
    paginate_by = 10
    
    def get_queryset(self):
        # Always exclude archived departments
        departments_queryset = Department.objects.filter(is_archived=False)
        # Also filter archived programs in counts
        program_filter = Q(program__is_archived=False)
        return departments_queryset.annotate(
            program_count=Count('program', filter=program_filter, distinct=True),
            staff_count=Count('program__programstaff__staff', filter=Q(program__programstaff__program__is_archived=False), distinct=True)
        ).order_by('name')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['total_departments'] = self.get_queryset().count()
        programs_queryset = Program.objects.all()
        # Exclude archived programs for non-admin users
        if not can_see_archived(self.request.user):
            programs_queryset = programs_queryset.filter(is_archived=False)
        context['total_programs'] = programs_queryset.count()
        context['total_staff'] = Staff.objects.count()
        return context


class DepartmentDetailView(AnalystAccessMixin, DetailView):
    model = Department
    template_name = 'core/department_detail.html'
    context_object_name = 'department'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'


class DepartmentCreateView(AnalystAccessMixin, CreateView):
    model = Department
    template_name = 'core/department_form.html'
    fields = ['name', 'owner']
    success_url = reverse_lazy('core:departments')
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to create departments"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Manager role users cannot create departments
                if 'Manager' in role_names and not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    from django.contrib import messages
                    messages.error(request, 'You do not have permission to create departments. Contact your administrator.')
                    return redirect('core:departments')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Get all staff members with Leader role
        from core.models import Staff, Role
        try:
            leader_role = Role.objects.get(name='Leader')
            context['leaders'] = Staff.objects.filter(
                staffrole__role=leader_role
            ).distinct().order_by('first_name', 'last_name')
        except Role.DoesNotExist:
            context['leaders'] = Staff.objects.none()
        return context
    
    def form_valid(self, form):
        department = form.save(commit=False)
        
        # Set audit fields
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                first_name = staff.first_name or ''
                last_name = staff.last_name or ''
                user_name = f"{first_name} {last_name}".strip()
                if not user_name or user_name == ' ':
                    user_name = staff.email or staff.user.username if staff.user else 'System'
            except:
                user_name = 'System'
        else:
            user_name = 'System'
        
        department.created_by = user_name
        department.updated_by = user_name
        department.save()
        
        # If an owner is selected, create DepartmentLeaderAssignment
        if department.owner:
            from core.models import DepartmentLeaderAssignment
            DepartmentLeaderAssignment.objects.create(
                staff=department.owner,
                department=department,
                assigned_by=staff if self.request.user.is_authenticated and hasattr(self.request.user, 'staff_profile') else None,
                is_active=True,
                notes=f"Auto-assigned as department owner during creation"
            )
        
        messages.success(self.request, 'Department created successfully.')
        return super().form_valid(form)


class DepartmentUpdateView(AnalystAccessMixin, UpdateView):
    model = Department
    template_name = 'core/department_form.html'
    fields = ['name', 'owner']
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('core:departments')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Get all staff members with Leader role
        from core.models import Staff, Role
        try:
            leader_role = Role.objects.get(name='Leader')
            context['leaders'] = Staff.objects.filter(
                staffrole__role=leader_role
            ).distinct().order_by('first_name', 'last_name')
        except Role.DoesNotExist:
            context['leaders'] = Staff.objects.none()
        return context
    
    def form_valid(self, form):
        department = form.save(commit=False)
        old_owner = self.get_object().owner
        
        # Set audit fields
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                first_name = staff.first_name or ''
                last_name = staff.last_name or ''
                user_name = f"{first_name} {last_name}".strip()
                if not user_name or user_name == ' ':
                    user_name = staff.email or staff.user.username if staff.user else 'System'
            except:
                user_name = 'System'
        else:
            user_name = 'System'
        
        department.updated_by = user_name
        department.save()
        
        # Handle DepartmentLeaderAssignment changes
        from core.models import DepartmentLeaderAssignment
        
        # Deactivate old assignment if owner changed
        if old_owner and old_owner != department.owner:
            DepartmentLeaderAssignment.objects.filter(
                staff=old_owner,
                department=department,
                is_active=True
            ).update(is_active=False)
        
        # Create new assignment if new owner is selected
        if department.owner and department.owner != old_owner:
            # Check if assignment already exists (inactive)
            assignment, created = DepartmentLeaderAssignment.objects.get_or_create(
                staff=department.owner,
                department=department,
                defaults={
                    'assigned_by': staff if self.request.user.is_authenticated and hasattr(self.request.user, 'staff_profile') else None,
                    'is_active': True,
                    'notes': f"Assigned as department owner during update"
                }
            )
            if not created:
                # Reactivate existing assignment
                assignment.is_active = True
                assignment.assigned_by = staff if self.request.user.is_authenticated and hasattr(self.request.user, 'staff_profile') else None
                assignment.notes = f"Reassigned as department owner during update"
                assignment.save()
        
        messages.success(self.request, 'Department updated successfully.')
        return super().form_valid(form)


class DepartmentDeleteView(AnalystAccessMixin, DeleteView):
    model = Department
    template_name = 'core/department_confirm_delete.html'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('core:departments')
    
    def form_valid(self, form):
        """Override to soft delete (archive) department instead of hard delete"""
        department = self.get_object()
        
        # Create audit log entry before archiving
        try:
            from .models import create_audit_log
            create_audit_log(
                entity_name='Department',
                entity_id=department.external_id,
                action='archive',
                changed_by=self.request.user,
                diff_data={
                    'name': department.name,
                    'owner': str(department.owner) if department.owner else None,
                    'archived_by': self.request.user.get_full_name() or self.request.user.username
                }
            )
        except Exception as e:
            print(f"Error creating audit log for department archiving: {e}")
        
        # Soft delete: set is_archived=True and archived_at timestamp
        from django.utils import timezone
        department.is_archived = True
        department.archived_at = timezone.now()
        department.save()

        # Cascade archive programs and their enrollments in this department
        now_ts = timezone.now()
        user_name = self.request.user.get_full_name() or self.request.user.username if self.request.user.is_authenticated else 'System'
        programs = Program.objects.filter(department=department)
        # Archive programs
        programs.filter(is_archived=False).update(is_archived=True, archived_at=now_ts, updated_by=user_name)
        # Archive enrollments tied to any program in this department
        ClientProgramEnrollment.objects.filter(program__department=department, is_archived=False).update(
            is_archived=True,
            archived_at=now_ts,
            updated_by=user_name
        )
        
        messages.success(
            self.request, 
            f'Department {department.name} has been archived. You can restore it from the archived departments section.'
        )
        return redirect(self.success_url)


# Enrollment CRUD Views
@method_decorator(jwt_required, name='dispatch')
class EnrollmentListView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, ListView):
    model = ClientProgramEnrollment
    template_name = 'core/enrollments.html'
    context_object_name = 'enrollments'
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
    
    def get_accessible_programs(self):
        """Get all programs that the current user has access to based on their roles"""
        if not self.request.user or not self.request.user.is_authenticated:
            return Program.objects.filter(status='active', is_archived=False).order_by('name')
        
        try:
            staff = self.request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            # SuperAdmin and Admin see all programs
            if any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                return Program.objects.filter(status='active', is_archived=False).order_by('name')
            
            # Analyst sees all programs (for reporting purposes)
            elif 'Analyst' in role_names and not any(role in ['SuperAdmin', 'Manager', 'Leader', 'Staff'] for role in role_names):
                return Program.objects.filter(status='active', is_archived=False).order_by('name')
            
            # Manager sees programs they're assigned to
            elif staff.is_program_manager():
                return staff.get_assigned_programs().filter(status='active', is_archived=False).order_by('name')
            
            # Leader sees programs in departments they lead
            elif staff.is_leader():
                assigned_departments = staff.get_assigned_departments()
                return Program.objects.filter(
                    department__in=assigned_departments,
                    status='active',
                    is_archived=False
                ).distinct().order_by('name')
            
            # Staff sees programs where their assigned clients are enrolled
            elif 'Staff' in role_names:
                from staff.models import StaffClientAssignment
                assigned_client_ids = StaffClientAssignment.objects.filter(
                    staff=staff,
                    is_active=True
                ).values_list('client_id', flat=True)
                return Program.objects.filter(
                    clientprogramenrollment__client_id__in=assigned_client_ids,
                    status='active',
                    is_archived=False
                ).distinct().order_by('name')
            
            # Default: show all active programs
            else:
                return Program.objects.filter(status='active', is_archived=False).order_by('name')
                
        except Exception:
            # If there's any error, show all active programs
            return Program.objects.filter(status='active', is_archived=False).order_by('name')
    
    def get_queryset(self):
        # First apply the ProgramManagerAccessMixin filtering
        queryset = super().get_queryset().select_related('client', 'program', 'program__department')
        
        # Always exclude archived enrollments
        queryset = queryset.filter(is_archived=False)
        
        # Apply date range filtering (start_date maps to intake_date, end_date maps to discharge_date)
        start_date, end_date, parsed_start_date, parsed_end_date = get_date_range_filter(self.request)
        
        if parsed_start_date:
            queryset = queryset.filter(start_date__gte=parsed_start_date)
        if parsed_end_date:
            queryset = queryset.filter(
                Q(end_date__lte=parsed_end_date) |
                Q(end_date__isnull=True, start_date__lte=parsed_end_date)
            )
        
        # Apply additional filters (excluding status for now)
        department_filter = self.request.GET.get('department', '')
        status_filter = self.request.GET.get('status', '')
        client_search = self.request.GET.get('client_search', '').strip()
        program_filter = self.request.GET.get('program', '').strip()
        client_sort = self.request.GET.get('client_sort', '').strip()
        
        if department_filter:
            queryset = queryset.filter(program__department__name=department_filter)
        
        if client_search:
            queryset = queryset.filter(
                Q(client__first_name__icontains=client_search) |
                Q(client__last_name__icontains=client_search) |
                Q(client__preferred_name__icontains=client_search) |
                Q(client__alias__icontains=client_search)
            ).distinct()
        
        if program_filter:
            try:
                program_id = int(program_filter)
                queryset = queryset.filter(program_id=program_id)
            except (ValueError, TypeError):
                pass
        
        today = timezone.now().date()
        queryset = queryset.annotate(
            computed_status=Case(
                When(is_archived=True, then=Value('archived')),
                When(status__in=['cancelled', 'suspended'], then=F('status')),
                When(start_date__gt=today, then=Value('pending')),
                When(Q(end_date__isnull=False) & Q(end_date__lt=today), then=Value('completed')),
                default=Value('active'),
                output_field=CharField()
            )
        )
        
        if status_filter:
            if status_filter == 'active_only':
                queryset = queryset.exclude(computed_status='archived')
            else:
                queryset = queryset.filter(computed_status=status_filter)
        
        # Apply client sorting with case-insensitive ordering (first name, then last name)
        from django.db.models.functions import Lower
        if client_sort == 'name_asc':
            queryset = queryset.annotate(
                client_first_name_lower=Lower('client__first_name'),
                client_last_name_lower=Lower('client__last_name')
            ).order_by('client_first_name_lower', 'client_last_name_lower', '-start_date')
        elif client_sort == 'name_desc':
            queryset = queryset.annotate(
                client_first_name_lower=Lower('client__first_name'),
                client_last_name_lower=Lower('client__last_name')
            ).order_by('-client_first_name_lower', '-client_last_name_lower', '-start_date')
        else:
            queryset = queryset.order_by('-start_date')
        
        self._filtered_enrollments = queryset
        return queryset
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        filtered_queryset = getattr(self, '_filtered_enrollments', self.get_queryset())
        
        total_filtered_count = filtered_queryset.count()
        
        # Calculate counts from ALL enrollments (excluding archived for non-admin users) for users with full access
        # This ensures the statistics show the true system-wide counts
        today = timezone.now().date()
        all_enrollments = ClientProgramEnrollment.objects.filter(is_archived=False)
        
        # Check if user has full access (SuperAdmin, Admin, Analyst, or no restrictions)
        has_full_access = False
        if self.request.user.is_authenticated:
            if self.request.user.is_superuser:
                has_full_access = True
            else:
                try:
                    staff = self.request.user.staff_profile
                    user_roles = staff.staffrole_set.select_related('role').all()
                    role_names = [staff_role.role.name for staff_role in user_roles]
                    # SuperAdmin, Admin, and Analyst should see all counts
                    if any(role in ['SuperAdmin', 'Admin', 'Analyst'] for role in role_names):
                        has_full_access = True
                except Exception:
                    pass
        
        if has_full_access:
            # Use all enrollments for counts
            counts_queryset = all_enrollments
        else:
            # Use filtered queryset for counts (respects user permissions)
            counts_queryset = filtered_queryset
        
        # Calculate status counts using date-based logic
        active_count = counts_queryset.filter(
            start_date__lte=today
        ).filter(
            Q(end_date__isnull=True) | Q(end_date__gt=today)
        ).exclude(
            status__in=['cancelled', 'suspended']
        ).count()
        
        completed_count = counts_queryset.filter(
            end_date__isnull=False,
            end_date__lt=today
        ).exclude(
            status__in=['cancelled', 'suspended']
        ).count()
        
        pending_count = counts_queryset.filter(
            start_date__gt=today
        ).exclude(
            status__in=['cancelled', 'suspended']
        ).count()
        
        total_enrollments = counts_queryset.exclude(
            status__in=['cancelled', 'suspended']
        ).count()
        
        context['total_enrollments'] = total_enrollments
        context['active_enrollments'] = active_count
        context['completed_enrollments'] = completed_count
        context['pending_enrollments'] = pending_count
        context['total_filtered_count'] = total_filtered_count
        
        success_rate = 0
        if total_enrollments > 0:
            success_rate = round((completed_count / total_enrollments) * 100, 1)
        context['success_rate'] = success_rate
        
        # Exclude archived departments and HASS from department dropdown
        context['departments'] = Department.objects.filter(
            is_archived=False
        ).exclude(
            name__iexact='HASS'
        ).order_by('name')
        context['status_choices'] = [
            ('', 'All Statuses'),
            ('pending', 'Pending'),
            ('active', 'Active'),
            ('completed', 'Completed'),
            ('cancelled', 'Cancelled'),
            ('suspended', 'Suspended'),
            ('archived', 'Archived'),
            ('active_only', 'All Non-Archived'),
        ]
        
        # Add date range filter parameters
        start_date, end_date, parsed_start_date, parsed_end_date = get_date_range_filter(self.request)
        context['start_date'] = start_date
        context['end_date'] = end_date
        
        # Get accessible programs for dropdown (similar to ClientListView)
        context['programs'] = self.get_accessible_programs()
        
        # Add current filter values
        context['current_department'] = self.request.GET.get('department', '')
        context['current_status'] = self.request.GET.get('status', '')
        context['client_search'] = self.request.GET.get('client_search', '')
        context['current_program'] = self.request.GET.get('program', '')
        context['client_sort'] = self.request.GET.get('client_sort', '')
        context['per_page'] = self.request.GET.get('per_page', '10')
        
        # Force pagination to be enabled if there are any results
        if context.get('paginator') and context['paginator'].count > 0:
            context['is_paginated'] = True
        
        return context


@method_decorator(jwt_required, name='dispatch')
class EnrollmentDetailView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, DetailView):
    model = ClientProgramEnrollment
    template_name = 'core/enrollment_detail.html'
    context_object_name = 'enrollment'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    
    def get_queryset(self):
        """Only serve non-archived enrollments"""
        return super().get_queryset().filter(is_archived=False)
    
    def get(self, request, *args, **kwargs):
        """Override get method to check access before showing enrollment details"""
        try:
            # Get the enrollment object
            self.object = self.get_object()
            
            # Check if user has access to this enrollment
            if not self.user_has_access_to_enrollment(request, self.object):
                return redirect(f"{reverse('core:permission_error')}?type=access_denied&resource=enrollment")
            
            # If user has access, proceed with normal rendering
            context = self.get_context_data(object=self.object)
            return self.render_to_response(context)
            
        except ClientProgramEnrollment.DoesNotExist:
            # If enrollment doesn't exist, show 404
            return redirect(f"{reverse('core:permission_error')}?type=enrollment_not_found&resource=enrollment")
        except Exception as e:
            # For any other error, show access denied
            return redirect(f"{reverse('core:permission_error')}?type=access_denied&resource=enrollment")
    
    def user_has_access_to_enrollment(self, request, enrollment):
        """Check if user has access to view this specific enrollment"""
        if not request.user.is_authenticated:
            return False
        
        # Superadmin can access everything
        if request.user.is_superuser:
            return True
        
        try:
            staff = request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            # Check if user is Analyst - they can see all enrollments
            if 'Analyst' in role_names and not any(role in ['SuperAdmin', 'Manager', 'Leader', 'Staff'] for role in role_names):
                return True
            
            # Check if user is Program Manager
            if staff.is_program_manager():
                assigned_programs = staff.get_assigned_programs()
                return enrollment.program in assigned_programs
            
            # Check if user is Leader
            elif staff.is_leader():
                assigned_departments = Department.objects.filter(
                    leader_assignments__staff=staff,
                    leader_assignments__is_active=True,
                    is_archived=False
                ).distinct()
                assigned_programs = Program.objects.filter(
                    department__in=assigned_departments
                ).distinct()
                return enrollment.program in assigned_programs
            
            # Check if user is Staff
            elif 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager', 'Leader'] for role in role_names):
                # Staff users can only see enrollments for their assigned clients
                from staff.models import StaffClientAssignment
                assigned_clients = Client.objects.filter(
                    staff_assignments__staff=staff,
                    staff_assignments__is_active=True
                ).distinct()
                return enrollment.client in assigned_clients
            
            # If no specific role matched, deny access
            return False
            
        except Exception:
            return False


@method_decorator(jwt_required, name='dispatch')
class EnrollmentCreateView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, CreateView):
    model = ClientProgramEnrollment
    form_class = EnrollmentForm
    template_name = 'core/enrollment_form.html'
    success_url = reverse_lazy('core:enrollments')
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to create enrollments"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot create enrollments
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Leader'] for role in role_names):
                    messages.error(request, 'You do not have permission to create enrollments. Contact your administrator.')
                    return redirect('core:enrollments')
                
                # Manager role users cannot create enrollments
                if 'Manager' in role_names and not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                    messages.error(request, 'You do not have permission to create enrollments. Contact your administrator.')
                    return redirect('core:enrollments')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def post(self, request, *args, **kwargs):
        """Override post method to handle form validation"""
        form = self.get_form()
        
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)
    
    def form_valid(self, form):
        """Override form_valid to set created_by and add audit logging"""
        try:
            enrollment = form.save(commit=False)
            enrollment.created_by = self.request.user.get_full_name() or self.request.user.username
            enrollment.save()
            
            # Create audit log entry
            try:
                from .models import create_audit_log
                create_audit_log(
                    entity_name='Enrollment',
                    entity_id=enrollment.external_id,
                    action='create',
                    changed_by=self.request.user,
                    diff_data={
                        'client': str(enrollment.client),
                        'program': str(enrollment.program),
                        'start_date': str(enrollment.start_date),
                        'status': enrollment.status
                    }
                )
            except Exception as e:
                # Log error but don't break the enrollment creation
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error creating audit log for enrollment: {e}")
            
            messages.success(self.request, 'Enrollment created successfully.')
            return super().form_valid(form)
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error in form_valid method: {e}")
            # Still call super to maintain normal behavior
            return super().form_valid(form)
    
    def get_form_kwargs(self):
        """Filter programs and clients to only assigned ones for Managers"""
        kwargs = super().get_form_kwargs()
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                if staff.is_program_manager():
                    # Filter programs to only assigned ones
                    assigned_programs = staff.get_assigned_programs().filter(is_archived=False)
                    kwargs['program_queryset'] = assigned_programs
                elif staff.is_leader():
                    # Filter programs to only assigned ones via departments
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True
                    ).distinct()
                    assigned_programs = Program.objects.filter(
                        department__in=assigned_departments,
                        is_archived=False
                    ).distinct()
                    kwargs['program_queryset'] = assigned_programs
                    
                    # For now, show all clients - program managers can enroll any client
                    # In the future, you might want to filter clients based on some criteria
                    kwargs['client_queryset'] = Client.objects.all()
            except Exception:
                pass
        return kwargs
    
    def form_invalid(self, form):
        """Handle form validation errors"""
        print("=== ENROLLMENT FORM_INVALID CALLED ===")
        print(f"Form errors: {form.errors}")
        
        # Add error messages to the form
        for field, errors in form.errors.items():
            for error in errors:
                if field == '__all__':
                    messages.error(self.request, str(error))
                else:
                    messages.error(self.request, f"{field}: {error}")
        
        # Get context data without calling the problematic parent method
        context = {
            'form': form,
            'view': self,
        }
        
        # Add program manager context if needed
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                if staff.is_program_manager():
                    context['assigned_programs'] = staff.get_assigned_programs()
                    context['is_program_manager'] = True
            except Exception:
                pass
        
        # Return the form with errors
        return self.render_to_response(context)


@method_decorator(jwt_required, name='dispatch')
class EnrollmentUpdateView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, UpdateView):
    model = ClientProgramEnrollment
    form_class = EnrollmentForm
    template_name = 'core/enrollment_form.html'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('core:enrollments')
    
    def get_queryset(self):
        """Restrict updates to non-archived enrollments"""
        return super().get_queryset().filter(is_archived=False)
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to edit enrollments"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot edit enrollments
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    messages.error(request, 'You do not have permission to edit enrollments. Contact your administrator.')
                    return redirect('core:enrollments')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_form_kwargs(self):
        """Filter programs and clients to only assigned ones for Managers"""
        kwargs = super().get_form_kwargs()
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                if staff.is_program_manager():
                    # Filter programs to only assigned ones
                    assigned_programs = staff.get_assigned_programs().filter(is_archived=False)
                    kwargs['program_queryset'] = assigned_programs
                elif staff.is_leader():
                    # Filter programs to only assigned ones via departments
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True
                    ).distinct()
                    assigned_programs = Program.objects.filter(
                        department__in=assigned_departments,
                        is_archived=False
                    ).distinct()
                    kwargs['program_queryset'] = assigned_programs
                    
                    # For now, show all clients - program managers can enroll any client
                    # In the future, you might want to filter clients based on some criteria
                    kwargs['client_queryset'] = Client.objects.all()
            except Exception:
                pass
        return kwargs
    
    def get_context_data(self, **kwargs):
        """Add client and program data for edit mode"""
        context = super().get_context_data(**kwargs)
        if self.object:
            # Add client data for JavaScript
            context['client_data'] = json.dumps({
                'id': self.object.client.id,
                'external_id': str(self.object.client.external_id),
                'name': f"{self.object.client.first_name} {self.object.client.last_name}",
                'first_name': self.object.client.first_name,
                'last_name': self.object.client.last_name,
            })
            # Add program data for JavaScript
            context['program_data'] = json.dumps({
                'id': self.object.program.id,
                'external_id': str(self.object.program.external_id),
                'name': self.object.program.name,
                'department': self.object.program.department.name,
                'location': self.object.program.location,
                'capacity': self.object.program.capacity_current,
                'description': self.object.program.description or '',
            })
        return context
    
    def form_valid(self, form):
        """Set the updated_by field before saving and add audit logging"""
        enrollment = form.save(commit=False)
        enrollment.updated_by = self.request.user.get_full_name() or self.request.user.username
        enrollment.save()
        
        # Create audit log entry
        from .models import create_audit_log
        create_audit_log(
            entity_name='Enrollment',
            entity_id=enrollment.external_id,
            action='update',
            changed_by=self.request.user,
            diff_data={
                'client': str(enrollment.client),
                'program': str(enrollment.program),
                'start_date': str(enrollment.start_date),
                'end_date': str(enrollment.end_date) if enrollment.end_date else None,
                'status': enrollment.status
            }
        )
        
        messages.success(self.request, 'Enrollment updated successfully.')
        return redirect(self.success_url)


@method_decorator(jwt_required, name='dispatch')
class EnrollmentDeleteView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, DeleteView):
    model = ClientProgramEnrollment
    template_name = 'core/enrollment_confirm_delete.html'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('core:enrollments')
    
    def get_queryset(self):
        """Restrict deletes to non-archived enrollments"""
        return super().get_queryset().filter(is_archived=False)
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to archive enrollments"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot archive enrollments
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    messages.error(request, 'You do not have permission to archive enrollments. Contact your administrator.')
                    return redirect('core:enrollments')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_context_data(self, **kwargs):
        """Add enrollment object to context"""
        context = super().get_context_data(**kwargs)
        context['enrollment'] = self.get_object()
        return context
    
    def form_valid(self, form):
        """Override to soft delete (archive) enrollment instead of hard delete"""
        try:
            # Get the enrollment object first
            enrollment = self.get_object()
            client_name = f"{enrollment.client.first_name} {enrollment.client.last_name}"
            program_name = enrollment.program.name
            
            # Create audit log entry before archiving
            from .models import create_audit_log
            create_audit_log(
                entity_name='Enrollment',
                entity_id=enrollment.external_id,
                action='archive',
                changed_by=self.request.user,
                diff_data={
                    'client': str(enrollment.client),
                    'program': str(enrollment.program),
                    'start_date': str(enrollment.start_date),
                    'end_date': str(enrollment.end_date) if enrollment.end_date else None,
                    'status': enrollment.status,
                    'archived_by': self.request.user.get_full_name() or self.request.user.username
                }
            )
            
            # Soft delete: set is_archived=True and archived_at timestamp
            from django.utils import timezone
            enrollment.is_archived = True
            enrollment.archived_at = timezone.now()
            user_name = self.request.user.get_full_name() or self.request.user.username if self.request.user.is_authenticated else 'System'
            enrollment.updated_by = user_name
            enrollment.save()
            
            messages.success(
                self.request, 
                f'Enrollment for {client_name} in {program_name} has been archived. You can restore it from the archived enrollments section.'
            )
            return redirect(self.success_url)
            
        except Exception as e:
            print(f"Error archiving enrollment: {str(e)}")
            messages.error(self.request, f'Error archiving enrollment: {str(e)}')
            return redirect(self.success_url)


@csrf_exempt
@require_http_methods(["POST"])
def check_program_capacity(request):
    """API endpoint to check program capacity for a specific date"""
    try:
        data = json.loads(request.body)
        program_id = data.get('program_id')
        start_date_str = data.get('start_date')
        
        if not program_id or not start_date_str:
            return JsonResponse({
                'success': False,
                'error': 'Program ID and start date are required'
            }, status=400)
        
        # Parse the date
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        except ValueError:
            return JsonResponse({
                'success': False,
                'error': 'Invalid date format. Use YYYY-MM-DD'
            }, status=400)
        
        # Get the program
        try:
            program = Program.objects.get(id=program_id, is_archived=False)
        except Program.DoesNotExist:
            return JsonResponse({
                'success': False,
                'error': 'Program not found'
            }, status=404)
        
        # Get capacity information for the specific date
        enrollments_on_date = program.get_enrollments_count_for_date(start_date)
        available_capacity = program.get_available_capacity(start_date)
        is_at_capacity = program.is_at_capacity(start_date)
        capacity_percentage = program.get_capacity_percentage(start_date)
        
        return JsonResponse({
            'success': True,
            'program_name': program.name,
            'capacity': program.capacity_current,
            'enrollments_on_date': enrollments_on_date,
            'available_capacity': available_capacity,
            'is_at_capacity': is_at_capacity,
            'capacity_percentage': round(capacity_percentage, 1),
            'start_date_formatted': start_date.strftime('%B %d, %Y')
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Server error: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["GET"])
def search_clients(request):
    """Search clients by name via AJAX with priority ordering"""
    try:
        query = request.GET.get('q', '').strip()
        
        if not query:
            # If no query, return all clients (limit to 20 for performance)
            clients = Client.objects.all()[:20]
        else:
            # First get clients that start with the query (highest priority)
            starts_with_clients = Client.objects.filter(
                Q(first_name__istartswith=query) |
                Q(last_name__istartswith=query)
            )
            
            # Then get clients that contain the query anywhere (lower priority)
            contains_clients = Client.objects.filter(
                Q(first_name__icontains=query) |
                Q(last_name__icontains=query)
            ).exclude(
                Q(first_name__istartswith=query) |
                Q(last_name__istartswith=query)
            )
            
            # Combine and limit results
            clients = list(starts_with_clients) + list(contains_clients)
            clients = clients[:50]  # Limit to 50 results for performance
        
        # Format results for the frontend
        results = []
        for client in clients:
            # Use numeric CCD ID (client_id) if available, otherwise fall back to external_id
            display_id = client.client_id if client.client_id else str(client.external_id)
            results.append({
                'id': client.id,
                'external_id': str(client.external_id),
                'client_id': client.client_id,  # Add the actual client_id field
                'name': f"{client.first_name} {client.last_name}",
                'first_name': client.first_name,
                'last_name': client.last_name,
                'date_of_birth': client.dob.strftime('%Y-%m-%d') if client.dob else None,
                'display_text': f"{client.first_name} {client.last_name} ({client.dob.strftime('%m/%d/%Y') if client.dob else 'No DOB'})",
                'display_id': display_id  # Add display_id for template use
            })
        
        return JsonResponse({
            'success': True,
            'clients': results,
            'count': len(results)
        })
        
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error searching clients: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["GET"])
def search_staff(request):
    """Search staff members by name via AJAX with priority ordering"""
    try:
        from .models import Staff
        query = request.GET.get('q', '').strip()
        
        # Filter to only active staff members with user accounts
        queryset = Staff.objects.filter(active=True, user__isnull=False).select_related('user')
        
        if not query:
            # If no query, return all active staff (limit to 20 for performance)
            staff_members = queryset.order_by('first_name', 'last_name')[:20]
        else:
            # First get staff that start with the query (highest priority)
            starts_with_staff = queryset.filter(
                Q(first_name__istartswith=query) |
                Q(last_name__istartswith=query) |
                Q(user__first_name__istartswith=query) |
                Q(user__last_name__istartswith=query)
            )
            
            # Then get staff that contain the query anywhere (lower priority)
            contains_staff = queryset.filter(
                Q(first_name__icontains=query) |
                Q(last_name__icontains=query) |
                Q(email__icontains=query) |
                Q(user__first_name__icontains=query) |
                Q(user__last_name__icontains=query) |
                Q(user__email__icontains=query)
            ).exclude(
                Q(first_name__istartswith=query) |
                Q(last_name__istartswith=query) |
                Q(user__first_name__istartswith=query) |
                Q(user__last_name__istartswith=query)
            )
            
            # Combine and limit results
            staff_members = list(starts_with_staff) + list(contains_staff)
            staff_members = staff_members[:50]  # Limit to 50 results for performance
        
        # Format results for the frontend
        results = []
        for staff in staff_members:
            # Use staff name if available, otherwise use user name
            first_name = staff.first_name or staff.user.first_name or ''
            last_name = staff.last_name or staff.user.last_name or ''
            email = staff.email or staff.user.email or ''
            name = f"{first_name} {last_name}".strip()
            if not name:
                name = staff.user.username or email
            
            results.append({
                'id': staff.id,
                'name': name,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'display_text': f"{name} ({email})" if email else name
            })
        
        return JsonResponse({
            'success': True,
            'staff': results,
            'count': len(results)
        })
        
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error searching staff: {str(e)}'
        }, status=500)


@login_required
def approve_restriction(request, external_id):
    """Approve a service restriction (SuperAdmin/Admin only)"""
    from django.shortcuts import get_object_or_404, redirect
    from django.contrib import messages
    from django.utils import timezone
    from .models import ServiceRestriction
    
    try:
        restriction = get_object_or_404(ServiceRestriction, external_id=external_id)
        
        # Check if user is SuperAdmin/Admin
        if not request.user.is_authenticated:
            messages.error(request, 'You must be logged in to approve restrictions.')
            return redirect('core:restrictions_detail', external_id=external_id)
        
        try:
            staff = request.user.staff_profile
            user_roles = staff.staffrole_set.select_related('role').all()
            role_names = [staff_role.role.name for staff_role in user_roles]
            
            if not any(role in ['SuperAdmin', 'Admin'] for role in role_names):
                messages.error(request, 'You do not have permission to approve restrictions. Only SuperAdmin and Admin can approve.')
                return redirect('core:restrictions_detail', external_id=external_id)
        except Exception:
            messages.error(request, 'Error checking permissions.')
            return redirect('core:restrictions_detail', external_id=external_id)
        
        # Approve the restriction
        restriction.is_approved = True
        restriction.approved_by = request.user.staff_profile
        restriction.approved_at = timezone.now()
        restriction.save()
        
        # Mark related notifications as read
        from .models import Notification
        Notification.objects.filter(
            category='restriction_approval',
            metadata__restriction_external_id=str(external_id),
            is_read=False
        ).update(is_read=True, read_at=timezone.now())
        
        messages.success(request, f'Service restriction for {restriction.client.first_name} {restriction.client.last_name} has been approved and is now active.')
        
        return redirect('core:restrictions_detail', external_id=external_id)
        
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error approving restriction {external_id}: {str(e)}")
        messages.error(request, f'Error approving restriction: {str(e)}')
        return redirect('core:restrictions_detail', external_id=external_id)


# Service Restriction CRUD Views
# Note: RestrictionListView is defined earlier in the file (line 255) with full pagination support


class RestrictionDetailView(AnalystAccessMixin, ProgramManagerAccessMixin, DetailView):
    model = ServiceRestriction
    template_name = 'core/restriction_detail.html'
    context_object_name = 'restriction'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    
    def get(self, request, *args, **kwargs):
        """Override get method to handle permission checks before rendering"""
        try:
            # First, try to get the object
            self.object = self.get_object()
        except Exception:
            # If object doesn't exist, redirect to permission error
            from django.shortcuts import redirect
            from django.urls import reverse
            return redirect(f"{reverse('core:permission_error')}?type=restriction_not_found&resource=restriction")
        
        # Check if user has access to this restriction
        if not request.user.is_superuser:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                if staff.is_program_manager():
                    # Managers can view ALL restrictions (no access restriction for viewing)
                    pass
                
                elif 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager', 'Leader'] for role in role_names):
                    # Staff users can view all restrictions (no additional filtering needed)
                    pass
                
                elif staff.is_leader():
                    # Leaders can view ALL restrictions (like Staff users)
                    # They can only edit restrictions for their assigned clients
                    pass
                        
            except Exception:
                from django.shortcuts import redirect
                from django.urls import reverse
                return redirect(f"{reverse('core:permission_error')}?type=access_denied&resource=restriction")
        
        # If we get here, user has access, proceed with normal rendering
        context = self.get_context_data(object=self.object)
        return self.render_to_response(context)
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        restriction = self.get_object()
        
        # Check if user is SuperAdmin/Admin and can approve
        can_approve = False
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                can_approve = any(role in ['SuperAdmin', 'Admin'] for role in role_names)
            except Exception:
                pass
        
        # Check if user can edit this restriction
        can_edit = False
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                if staff.is_program_manager():
                    # Managers can edit restrictions for their assigned clients
                    assigned_programs = staff.get_assigned_programs()
                    assigned_clients = Client.objects.filter(
                        clientprogramenrollment__program__in=assigned_programs
                    ).distinct()
                    can_edit = restriction.client in assigned_clients
                elif staff.is_leader():
                    # Leaders can edit restrictions for their assigned clients
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True
                    ).distinct()
                    assigned_programs = Program.objects.filter(
                        department__in=assigned_departments
                    ).distinct()
                    assigned_clients = Client.objects.filter(
                        clientprogramenrollment__program__in=assigned_programs
                    ).distinct()
                    can_edit = restriction.client in assigned_clients
                elif staff.is_staff_only():
                    # Staff users can view all restrictions but cannot edit them
                    can_edit = False
                elif 'Admin' in role_names or self.request.user.is_superuser:
                    # Admin and SuperAdmin can edit all restrictions
                    can_edit = True
            except Exception:
                pass
        
        # Check if user is SuperAdmin/Admin and can approve
        can_approve = False
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                can_approve = any(role in ['SuperAdmin', 'Admin'] for role in role_names)
            except Exception:
                pass
        
        context['can_edit_restriction'] = can_edit
        context['can_approve'] = can_approve
        return context


class RestrictionCreateView(AnalystAccessMixin, ProgramManagerAccessMixin, CreateView):
    model = ServiceRestriction
    template_name = 'core/restriction_form.html'
    form_class = ServiceRestrictionForm
    success_url = reverse_lazy('core:restrictions')
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to create restrictions"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot create restrictions
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager'] for role in role_names):
                    messages.error(request, 'You do not have permission to create restrictions. Contact your administrator.')
                    return redirect('core:restrictions')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_form_kwargs(self):
        """Filter programs to only assigned ones for Managers"""
        kwargs = super().get_form_kwargs()
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                if staff.is_program_manager():
                    # Filter programs to only assigned ones
                    assigned_programs = staff.get_assigned_programs().order_by('name')
                    kwargs['program_queryset'] = assigned_programs
                elif staff.is_leader():
                    # Filter programs to only assigned ones via departments
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True
                    ).distinct()
                    assigned_programs = Program.objects.filter(
                        department__in=assigned_departments
                    ).distinct().order_by('name')
                    kwargs['program_queryset'] = assigned_programs
            except Exception:
                pass
        return kwargs
    
    def get_initial(self):
        """Set initial values for the form"""
        initial = super().get_initial()
        if self.request.user.is_authenticated:
            try:
                if hasattr(self.request.user, 'staff_profile'):
                    initial['entered_by'] = self.request.user.staff_profile
            except Exception:
                pass
        return initial
    
    def form_valid(self, form):
        try:
            # Debug: Print POST data
            print(f"RestrictionCreateView.form_valid - POST data: {dict(self.request.POST)}")
            print(f"RestrictionCreateView.form_valid - behaviors in POST: {self.request.POST.getlist('behaviors')}")
            # Debug: Print form data before saving
            print(f"RestrictionCreateView.form_valid - form.cleaned_data: {form.cleaned_data}")
            print(f"RestrictionCreateView.form_valid - behaviors in cleaned_data: {form.cleaned_data.get('behaviors')}")
            
            # Set the created_by and updated_by fields before saving
            restriction = form.save(commit=False)
            user_name = self.request.user.get_full_name() or self.request.user.username
            restriction.created_by = user_name
            restriction.updated_by = user_name
            
            # Debug: Print instance data
            print(f"RestrictionCreateView.form_valid - restriction.behaviors before save: {restriction.behaviors}")
            print(f"RestrictionCreateView.form_valid - restriction.behaviors type: {type(restriction.behaviors)}")
            
            # Set entered_by to current user's staff profile if not provided
            if not restriction.entered_by and hasattr(self.request.user, 'staff_profile'):
                restriction.entered_by = self.request.user.staff_profile
            
            # Check if user is Manager or Leader (not SuperAdmin/Admin)
            user_roles = []
            try:
                staff = self.request.user.staff_profile
                user_roles = [staff_role.role.name for staff_role in staff.staffrole_set.select_related('role').all()]
            except Exception:
                pass
            
            is_manager_or_leader = ('Manager' in user_roles or 'Leader' in user_roles) and not any(role in ['SuperAdmin', 'Admin'] for role in user_roles)
            
            # Set approval status based on user role
            # Manager/Leader created restrictions need approval (inactive)
            # SuperAdmin/Admin created restrictions are auto-approved (active)
            if is_manager_or_leader:
                restriction.is_approved = False
                restriction.approved_by = None
                restriction.approved_at = None
            else:
                # SuperAdmin/Admin - auto-approve
                restriction.is_approved = True
                if hasattr(self.request.user, 'staff_profile'):
                    restriction.approved_by = self.request.user.staff_profile
                    restriction.approved_at = timezone.now()
            
            restriction.save()
            client_profile_image = form.cleaned_data.get('client_profile_image')
            if client_profile_image:
                client = restriction.client
                client.profile_picture = client_profile_image
                client.save(update_fields=['profile_picture'])
            
            # Send notifications
            create_service_restriction_notification(restriction, event_type='new')
            
            # If Manager/Leader created restriction, alert SuperAdmin for approval
            if is_manager_or_leader:
                from .notification_utils import notify_superadmin_for_approval
                notify_superadmin_for_approval(restriction, action='created', user=self.request.user)
                # Inform user that their restriction requires approval
                messages.info(
                    self.request, 
                    'Service restriction created successfully. It is now pending SuperAdmin approval and will be inactive until approved.'
                )
                # Inform user that their restriction requires approval
                messages.info(
                    self.request, 
                    'Service restriction created successfully. It is now pending SuperAdmin approval and will be inactive until approved.'
                )
            
            # Debug: Print after save
            print(f"RestrictionCreateView.form_valid - restriction.behaviors after save: {restriction.behaviors}")
            
            # Create audit log entry for restriction creation
            try:
                from .models import create_audit_log
                create_audit_log(
                    entity_name='Restriction',
                    entity_id=restriction.external_id,
                    action='create',
                    changed_by=self.request.user,
                    diff_data={
                        'client': str(restriction.client),
                        'scope': restriction.scope,
                        'program': str(restriction.program) if restriction.program else None,
                        'restriction_type': restriction.restriction_type,
                        'is_bill_168': restriction.is_bill_168,
                        'is_no_trespass': restriction.is_no_trespass,
                        'start_date': str(restriction.start_date),
                        'end_date': str(restriction.end_date) if restriction.end_date else None,
                        'is_indefinite': restriction.is_indefinite,
                        'behaviors': restriction.behaviors,
                        'notes': restriction.notes or '',
                        'created_by': restriction.created_by
                    }
                )
            except Exception as e:
                print(f"Error creating audit log for restriction: {e}")
            
            create_success(self.request, 'Service restriction')
            return redirect(self.success_url)
        except Exception as e:
            print(f"Error saving form: {e}")
            messages.error(self.request, f'Error creating restriction: {e}')
            return self.form_invalid(form)
    
    def form_invalid(self, form):
        print("RestrictionCreateView.form_invalid called")
        print(f"Form is valid: {form.is_valid()}")
        print(f"Form errors: {form.errors}")
        print(f"Form non_field_errors: {form.non_field_errors()}")
        for field_name, errors in form.errors.items():
            print(f"Field '{field_name}' errors: {errors}")
        return super().form_invalid(form)


class RestrictionUpdateView(AnalystAccessMixin, ProgramManagerAccessMixin, UpdateView):
    model = ServiceRestriction
    template_name = 'core/restriction_form.html'
    form_class = ServiceRestrictionForm
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('core:restrictions')
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to edit restrictions"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot edit restrictions (unless they have Admin, SuperAdmin, Manager, or Leader roles)
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Admin', 'Manager', 'Leader'] for role in role_names):
                    messages.error(request, 'You do not have permission to edit restrictions. Contact your administrator.')
                    return redirect('core:restrictions')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_form_kwargs(self):
        """Filter programs to only assigned ones for Managers"""
        kwargs = super().get_form_kwargs()
        if self.request.user.is_authenticated:
            try:
                staff = self.request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Admin and SuperAdmin can access all programs
                if 'Admin' in role_names or self.request.user.is_superuser:
                    # No program filtering - they can access all programs
                    pass
                elif staff.is_program_manager():
                    # Filter programs to only assigned ones
                    assigned_programs = staff.get_assigned_programs().order_by('name')
                    kwargs['program_queryset'] = assigned_programs
                elif staff.is_leader():
                    # Filter programs to only assigned ones via departments
                    assigned_departments = Department.objects.filter(
                        leader_assignments__staff=staff,
                        leader_assignments__is_active=True
                    ).distinct()
                    assigned_programs = Program.objects.filter(
                        department__in=assigned_departments
                    ).distinct().order_by('name')
                    kwargs['program_queryset'] = assigned_programs
            except Exception:
                pass
        return kwargs
    
    def form_valid(self, form):
        """Handle restriction updates with audit logging"""
        # Get the original restriction data before saving
        original_restriction = self.get_object()
        
        # Set the updated_by field before saving
        restriction = form.save(commit=False)
        user_name = self.request.user.get_full_name() or self.request.user.username
        restriction.updated_by = user_name
        
        # Set entered_by to current user's staff profile if not already set (for backward compatibility)
        if not restriction.entered_by and hasattr(self.request.user, 'staff_profile'):
            restriction.entered_by = self.request.user.staff_profile
        
        # Check if user is Manager or Leader (not SuperAdmin/Admin)
        user_roles = []
        try:
            staff = self.request.user.staff_profile
            user_roles = [staff_role.role.name for staff_role in staff.staffrole_set.select_related('role').all()]
        except Exception:
            pass
        
        is_manager_or_leader = ('Manager' in user_roles or 'Leader' in user_roles) and not any(role in ['SuperAdmin', 'Admin'] for role in user_roles)
        
        # Set approval status based on user role
        # Manager/Leader edited restrictions need approval (inactive) - ALWAYS require re-approval
        # SuperAdmin/Admin edited restrictions remain approved (if already approved) or get auto-approved
        if is_manager_or_leader:
            # If Manager/Leader edits ANY restriction (even if previously approved), set to unapproved
            # This ensures all Manager/Leader edits require SuperAdmin approval
            restriction.is_approved = False
            restriction.approved_by = None
            restriction.approved_at = None
        elif not restriction.is_approved:
            # If SuperAdmin/Admin edits an unapproved restriction, auto-approve it
            restriction.is_approved = True
            if hasattr(self.request.user, 'staff_profile'):
                restriction.approved_by = self.request.user.staff_profile
                restriction.approved_at = timezone.now()
        # Note: If SuperAdmin/Admin edits an already-approved restriction, 
        # is_approved remains True (no change needed - preserves existing approval)
        
        restriction.save()
        client_profile_image = form.cleaned_data.get('client_profile_image')
        if client_profile_image:
            client = restriction.client
            client.profile_picture = client_profile_image
            client.save(update_fields=['profile_picture'])
        
        # If Manager/Leader edited restriction, alert SuperAdmin for approval
        if is_manager_or_leader:
            from .notification_utils import notify_superadmin_for_approval
            notify_superadmin_for_approval(restriction, action='updated', user=self.request.user)
            # Inform user that their edit requires approval
            messages.info(
                self.request, 
                'Your changes have been saved. This restriction is now pending SuperAdmin approval and will be inactive until approved.'
            )

        if restriction.end_date and not restriction.is_indefinite:
            today = timezone.now().date()
            window_end = today + timedelta(days=7)
            if today <= restriction.end_date <= window_end:
                end_date_changed = original_restriction.end_date != restriction.end_date
                if end_date_changed or not Notification.objects.filter(
                    staff__isnull=False,
                    category='service_restriction',
                    metadata__restriction_external_id=str(restriction.external_id),
                    metadata__event_type='expiring'
                ).exists():
                    create_service_restriction_notification(restriction, event_type='expiring')
        
        # Create audit log entry for restriction update
        try:
            from .models import create_audit_log
            
            # Compare original and updated values to detect changes
            changes = {}
            
            # Check each field for changes
            if original_restriction.scope != restriction.scope:
                changes['scope'] = f"{original_restriction.scope} → {restriction.scope}"
            
            if original_restriction.program != restriction.program:
                changes['program'] = f"{str(original_restriction.program) if original_restriction.program else 'None'} → {str(restriction.program) if restriction.program else 'None'}"
            
            if original_restriction.restriction_type != restriction.restriction_type:
                changes['restriction_type'] = f"{original_restriction.restriction_type} → {restriction.restriction_type}"
            
            if original_restriction.is_bill_168 != restriction.is_bill_168:
                changes['is_bill_168'] = f"{original_restriction.is_bill_168} → {restriction.is_bill_168}"
            
            if original_restriction.is_no_trespass != restriction.is_no_trespass:
                changes['is_no_trespass'] = f"{original_restriction.is_no_trespass} → {restriction.is_no_trespass}"
            
            if original_restriction.start_date != restriction.start_date:
                changes['start_date'] = f"{original_restriction.start_date} → {restriction.start_date}"
            
            if original_restriction.end_date != restriction.end_date:
                changes['end_date'] = f"{original_restriction.end_date} → {restriction.end_date}"
            
            if original_restriction.is_indefinite != restriction.is_indefinite:
                changes['is_indefinite'] = f"{original_restriction.is_indefinite} → {restriction.is_indefinite}"
            
            if original_restriction.behaviors != restriction.behaviors:
                changes['behaviors'] = f"{original_restriction.behaviors} → {restriction.behaviors}"
            
            if original_restriction.notes != restriction.notes:
                changes['notes'] = f"{original_restriction.notes or ''} → {restriction.notes or ''}"
            
            # Only create audit log if there were changes
            if changes:
                create_audit_log(
                    entity_name='Restriction',
                    entity_id=restriction.external_id,
                    action='update',
                    changed_by=self.request.user,
                    diff_data=changes
                )
        except Exception as e:
            print(f"Error creating audit log for restriction update: {e}")
        
        update_success(self.request, 'Service restriction')
        return redirect(self.success_url)
    
    def get_context_data(self, **kwargs):
        """Add client data for pre-population in edit mode"""
        context = super().get_context_data(**kwargs)
        if self.object:
            # Add client data for JavaScript pre-population
            context['client_data'] = json.dumps({
                'id': self.object.client.id,
                'external_id': str(self.object.client.external_id),
                'name': f"{self.object.client.first_name} {self.object.client.last_name}",
                'first_name': self.object.client.first_name,
                'last_name': self.object.client.last_name,
            })
        return context


class RestrictionDeleteView(AnalystAccessMixin, ProgramManagerAccessMixin, DeleteView):
    model = ServiceRestriction
    template_name = 'core/restriction_confirm_delete.html'
    slug_field = 'external_id'
    slug_url_kwarg = 'external_id'
    success_url = reverse_lazy('core:restrictions')
    
    def dispatch(self, request, *args, **kwargs):
        """Check if user has permission to archive restrictions"""
        if request.user.is_authenticated:
            try:
                staff = request.user.staff_profile
                user_roles = staff.staffrole_set.select_related('role').all()
                role_names = [staff_role.role.name for staff_role in user_roles]
                
                # Staff role users cannot archive restrictions
                if 'Staff' in role_names and not any(role in ['SuperAdmin', 'Manager', 'Leader'] for role in role_names):
                    messages.error(request, 'You do not have permission to archive restrictions. Contact your administrator.')
                    return redirect('core:restrictions')
            except Exception:
                pass
        
        return super().dispatch(request, *args, **kwargs)
    
    def get_context_data(self, **kwargs):
        """Add restriction object to context"""
        context = super().get_context_data(**kwargs)
        context['restriction'] = self.get_object()
        return context
    
    def form_valid(self, form):
        """Handle restriction archiving with audit logging"""
        print("RestrictionDeleteView.form_valid called")
        
        # Get the restriction object first
        restriction = self.get_object()
        print(f"Restriction to archive: {restriction}")
        
        # Check confirmation text
        confirmation = self.request.POST.get('confirmation_text', '').strip().upper()
        if confirmation != 'ARCHIVE':
            messages.error(self.request, 'Please type "ARCHIVE" to confirm archiving.')
            return redirect('core:restrictions_detail', external_id=restriction.external_id)
        
        client_name = f"{restriction.client.first_name} {restriction.client.last_name}"
        restriction_type = restriction.get_restriction_type_display()
        
        # Create audit log entry before archiving
        print("Creating audit log for restriction archiving...")
        try:
            from .models import create_audit_log
            create_audit_log(
                entity_name='Restriction',
                entity_id=restriction.external_id,
                action='archive',
                changed_by=self.request.user,
                diff_data={
                    'client': str(restriction.client),
                    'scope': restriction.scope,
                    'program': str(restriction.program) if restriction.program else None,
                    'restriction_type': restriction.restriction_type,
                    'start_date': str(restriction.start_date),
                    'end_date': str(restriction.end_date) if restriction.end_date else None,
                    'is_indefinite': restriction.is_indefinite,
                    'behaviors': restriction.behaviors,
                    'notes': restriction.notes or '',
                    'is_archived': f"{restriction.is_archived} → True",
                    'archived_by': f"{self.request.user.first_name} {self.request.user.last_name}".strip() or self.request.user.username
                }
            )
            print("Audit log created for restriction archiving")
        except Exception as e:
            print(f"Error creating audit log for restriction archiving: {e}")
        
        # Archive the restriction instead of deleting
        print("Archiving restriction...")
        from django.utils import timezone
        restriction.is_archived = True
        restriction.archived_at = timezone.now()
        restriction.updated_by = f"{self.request.user.first_name} {self.request.user.last_name}".strip() or self.request.user.username
        restriction.save()
        print("Restriction archived successfully")
        
        messages.success(self.request, f'Service restriction for {client_name} has been archived successfully.')
        return redirect(self.success_url)


@csrf_exempt
@require_http_methods(["POST"])
def bulk_delete_restrictions(request):
    """Bulk archive service restrictions"""
    # Check if user is authenticated
    if not request.user.is_authenticated:
        return JsonResponse({
            'success': False,
            'error': 'Authentication required'
        }, status=401)
    
    try:
        data = json.loads(request.body)
        restriction_ids = data.get('restriction_ids', [])
        
        if not restriction_ids:
            return JsonResponse({
                'success': False,
                'error': 'No restrictions selected for archiving'
            }, status=400)
        
        # Get the restrictions to archive
        restrictions_to_archive = ServiceRestriction.objects.filter(id__in=restriction_ids)
        archived_count = restrictions_to_archive.count()
        
        if archived_count == 0:
            return JsonResponse({
                'success': False,
                'error': 'No restrictions found with the provided IDs'
            }, status=404)
        
        # Archive the restrictions instead of deleting
        from django.utils import timezone
        updated_by = f"{request.user.first_name} {request.user.last_name}".strip() or request.user.username
        archived_at = timezone.now()
        for restriction in restrictions_to_archive:
            restriction.is_archived = True
            restriction.archived_at = archived_at
            restriction.updated_by = updated_by
            restriction.save()
        
        return JsonResponse({
            'success': True,
            'archived_count': archived_count,
            'message': f'Successfully archived {archived_count} restriction(s)'
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error archiving restrictions: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def bulk_delete_departments(request):
    """Bulk delete departments"""
    # Check if user is authenticated
    if not request.user.is_authenticated:
        return JsonResponse({
            'success': False,
            'error': 'Authentication required'
        }, status=401)
    
    try:
        data = json.loads(request.body)
        department_ids = data.get('department_ids', [])
        
        if not department_ids:
            return JsonResponse({
                'success': False,
                'error': 'No departments selected for deletion'
            }, status=400)
        
        # Get the departments to delete
        departments_to_delete = Department.objects.filter(external_id__in=department_ids)
        deleted_count = departments_to_delete.count()
        
        if deleted_count == 0:
            return JsonResponse({
                'success': False,
                'error': 'No departments found with the provided IDs'
            }, status=404)
        
        # Actually delete the departments
        from .models import create_audit_log
        from django.utils import timezone
        now_ts = timezone.now()
        user_name = request.user.get_full_name() or request.user.username if request.user.is_authenticated else 'System'
        for department in departments_to_delete:
            # Create audit log entry before deletion
            create_audit_log(
                entity_name='Department',
                entity_id=department.external_id,
                action='delete',
                changed_by=request.user,
                diff_data={
                    'name': department.name,
                    'owner': department.owner,
                    'deleted_by': request.user.get_full_name() or request.user.username
                }
            )
            
            # Soft delete: set is_archived=True and archived_at timestamp
            department.is_archived = True
            department.archived_at = now_ts
            department.save()

            # Cascade archive programs and enrollments for this department
            programs = Program.objects.filter(department=department)
            programs.filter(is_archived=False).update(is_archived=True, archived_at=now_ts, updated_by=user_name)
            ClientProgramEnrollment.objects.filter(program__department=department, is_archived=False).update(
                is_archived=True,
                archived_at=now_ts,
                updated_by=user_name
            )
        
        return JsonResponse({
            'success': True,
            'deleted_count': deleted_count,
            'message': f'Successfully archived {deleted_count} department(s). You can restore them from the archived departments section.'
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error deleting departments: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def bulk_delete_enrollments(request):
    """Bulk delete client program enrollments"""
    # Check if user is authenticated
    if not request.user.is_authenticated:
        return JsonResponse({
            'success': False,
            'error': 'Authentication required'
        }, status=401)
    
    try:
        data = json.loads(request.body)
        enrollment_ids = data.get('enrollment_ids', [])
        
        if not enrollment_ids:
            return JsonResponse({
                'success': False,
                'error': 'No enrollments selected for deletion'
            }, status=400)
        
        # Get the enrollments to delete
        enrollments_to_delete = ClientProgramEnrollment.objects.filter(
            id__in=enrollment_ids,
            is_archived=False
        )
        deleted_count = enrollments_to_delete.count()
        
        if deleted_count == 0:
            return JsonResponse({
                'success': False,
                'error': 'No enrollments found with the provided IDs'
            }, status=404)
        
        # Soft delete: archive enrollments instead of actually deleting them
        from .models import create_audit_log
        user_name = request.user.get_full_name() or request.user.username if request.user.is_authenticated else 'System'
        
        for enrollment in enrollments_to_delete:
            # Create audit log entry before archiving
            create_audit_log(
                entity_name='Enrollment',
                entity_id=enrollment.external_id,
                action='archive',
                changed_by=request.user,
                diff_data={
                    'client': str(enrollment.client),
                    'program': str(enrollment.program),
                    'start_date': str(enrollment.start_date),
                    'end_date': str(enrollment.end_date) if enrollment.end_date else None,
                    'status': enrollment.status,
                    'archived_by': user_name
                }
            )
            
            # Soft delete: set is_archived=True and archived_at timestamp
            from django.utils import timezone
            enrollment.is_archived = True
            enrollment.archived_at = timezone.now()
            enrollment.updated_by = user_name
            enrollment.save()
        
        return JsonResponse({
            'success': True,
            'deleted_count': deleted_count,
            'message': f'Successfully archived {deleted_count} enrollment(s). You can restore them from the archived enrollments section.'
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error archiving enrollments: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@jwt_required
def bulk_restore_enrollments(request):
    """Bulk restore archived client program enrollments"""
    # Check if user is authenticated
    if not request.user.is_authenticated:
        return JsonResponse({
            'success': False,
            'error': 'Authentication required'
        }, status=401)
    
    try:
        data = json.loads(request.body)
        enrollment_ids = data.get('enrollment_ids', [])
        
        if not enrollment_ids:
            return JsonResponse({
                'success': False,
                'error': 'No enrollments selected for restoration'
            }, status=400)
        
        # Get the archived enrollments to restore
        enrollments_to_restore = ClientProgramEnrollment.objects.filter(
            id__in=enrollment_ids,
            is_archived=True
        )
        restored_count = enrollments_to_restore.count()
        
        if restored_count == 0:
            return JsonResponse({
                'success': False,
                'error': 'No archived enrollments found with the provided IDs'
            }, status=404)
        
        # Restore enrollments: set is_archived=False
        from .models import create_audit_log
        user_name = request.user.get_full_name() or request.user.username if request.user.is_authenticated else 'System'
        
        restored_items = []
        for enrollment in enrollments_to_restore:
            enrollment_name = f"{enrollment.client.first_name} {enrollment.client.last_name} - {enrollment.program.name}"
            restored_items.append(enrollment_name)
            
            # Create audit log entry for restoration
            create_audit_log(
                entity_name='Enrollment',
                entity_id=enrollment.external_id,
                action='restore',
                changed_by=request.user,
                diff_data={
                    'client': str(enrollment.client),
                    'program': str(enrollment.program),
                    'start_date': str(enrollment.start_date),
                    'end_date': str(enrollment.end_date) if enrollment.end_date else None,
                    'status': enrollment.status,
                    'restored_by': user_name,
                    'restored_item': enrollment_name
                }
            )
            
            # Restore: set is_archived=False and clear archived_at
            enrollment.is_archived = False
            enrollment.archived_at = None
            enrollment.updated_by = user_name
            enrollment.save()
        
        # Create a clear success message
        if restored_count == 1:
            message = f'Enrollment "{restored_items[0]}" has been restored successfully.'
        else:
            message = f'Successfully restored {restored_count} enrollment(s): ' + ', '.join(restored_items[:3])
            if restored_count > 3:
                message += f' and {restored_count - 3} more.'
        
        return JsonResponse({
            'success': True,
            'restored_count': restored_count,
            'message': message,
            'restored_items': restored_items
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error restoring enrollments: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@jwt_required
def bulk_restore_departments(request):
    """Bulk restore archived departments"""
    if not request.user.is_authenticated:
        return JsonResponse({
            'success': False,
            'error': 'Authentication required'
        }, status=401)
    
    try:
        data = json.loads(request.body)
        department_ids = data.get('department_ids', [])
        
        if not department_ids:
            return JsonResponse({
                'success': False,
                'error': 'No departments selected for restoration'
            }, status=400)
        
        # Get the archived departments to restore
        departments_to_restore = Department.objects.filter(
            external_id__in=department_ids,
            is_archived=True
        )
        restored_count = departments_to_restore.count()
        
        if restored_count == 0:
            return JsonResponse({
                'success': False,
                'error': 'No archived departments found with the provided IDs'
            }, status=404)
        
        # Restore departments: set is_archived=False and clear archived_at
        from .models import create_audit_log
        user_name = request.user.get_full_name() or request.user.username if request.user.is_authenticated else 'System'
        
        for department in departments_to_restore:
            # Create audit log entry for restoration
            create_audit_log(
                entity_name='Department',
                entity_id=department.external_id,
                action='restore',
                changed_by=request.user,
                diff_data={
                    'name': department.name,
                    'restored_by': user_name
                }
            )
            
            # Restore: set is_archived=False and clear archived_at
            department.is_archived = False
            department.archived_at = None
            department.save()
        
        return JsonResponse({
            'success': True,
            'restored_count': restored_count,
            'message': f'Successfully restored {restored_count} department(s)'
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error restoring departments: {str(e)}'
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
@jwt_required
def bulk_restore_restrictions(request):
    """Bulk restore archived service restrictions"""
    if not request.user.is_authenticated:
        return JsonResponse({
            'success': False,
            'error': 'Authentication required'
        }, status=401)
    
    try:
        data = json.loads(request.body)
        restriction_ids = data.get('restriction_ids', [])
        
        if not restriction_ids:
            return JsonResponse({
                'success': False,
                'error': 'No restrictions selected for restoration'
            }, status=400)
        
        # Get the archived restrictions to restore
        restrictions_to_restore = ServiceRestriction.objects.filter(
            id__in=restriction_ids,
            is_archived=True
        )
        restored_count = restrictions_to_restore.count()
        
        if restored_count == 0:
            return JsonResponse({
                'success': False,
                'error': 'No archived restrictions found with the provided IDs'
            }, status=404)
        
        # Restore restrictions: set is_archived=False and clear archived_at
        from .models import create_audit_log
        user_name = request.user.get_full_name() or request.user.username if request.user.is_authenticated else 'System'
        
        for restriction in restrictions_to_restore:
            # Create audit log entry for restoration
            create_audit_log(
                entity_name='Restriction',
                entity_id=restriction.external_id,
                action='restore',
                changed_by=request.user,
                diff_data={
                    'client': str(restriction.client),
                    'restored_by': user_name
                }
            )
            
            # Restore: set is_archived=False and clear archived_at
            restriction.is_archived = False
            restriction.archived_at = None
            restriction.updated_by = user_name
            restriction.save()
        
        return JsonResponse({
            'success': True,
            'restored_count': restored_count,
            'message': f'Successfully restored {restored_count} restriction(s)'
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'Error restoring restrictions: {str(e)}'
        }, status=500)


@jwt_required
def profile(request):
    """User profile view"""
    try:
        staff_profile = request.user.staff_profile
    except Staff.DoesNotExist:
        staff_profile = None

    context = {
        'user': request.user,
        'staff_profile': staff_profile,
    }
    
    return render(request, 'core/profile.html', context)


@jwt_required
def edit_profile(request):
    """Edit user profile view"""
    try:
        staff_profile = request.user.staff_profile
    except Staff.DoesNotExist:
        # Create a staff profile if it doesn't exist
        staff_profile = Staff.objects.create(
            user=request.user,
            first_name=request.user.first_name,
            last_name=request.user.last_name,
        )

    if request.method == 'POST':
        user_form = UserProfileForm(request.POST, request.FILES, instance=request.user)
        staff_form = StaffProfileForm(request.POST, instance=staff_profile)
        
        if user_form.is_valid() and staff_form.is_valid():
            user = user_form.save()
            staff = staff_form.save(commit=False)
            
            # Update staff name fields from user
            staff.first_name = user.first_name
            staff.last_name = user.last_name
            staff.save()
            
            messages.success(request, 'Profile updated successfully!')
            return redirect('core:profile')
        else:
            messages.error(request, 'Please correct the errors below.')
    else:
        user_form = UserProfileForm(instance=request.user)
        staff_form = StaffProfileForm(instance=staff_profile)

    context = {
        'user_form': user_form,
        'staff_form': staff_form,
        'user': request.user,
        'staff_profile': staff_profile,
    }
    
    return render(request, 'core/edit_profile.html', context)


@jwt_required
def test_messages(request):
    """Test page for message system"""
    return render(request, 'test_messages.html')


@jwt_required
def test_success(request):
    """Test success message"""
    success_message(request, "This is a test success message!")
    return redirect('core:test_messages')


@jwt_required
def test_error(request):
    """Test error message"""
    error_message(request, "This is a test error message!")
    return redirect('core:test_messages')


@jwt_required
def test_warning(request):
    """Test warning message"""
    warning_message(request, "This is a test warning message!")
    return redirect('core:test_messages')


@jwt_required
def test_info(request):
    """Test info message"""
    info_message(request, "This is a test info message!")
    return redirect('core:test_messages')


@jwt_required
def test_create_success(request):
    """Test create success message"""
    create_success(request, 'Test Entity', 'Sample Item')
    return redirect('core:test_messages')


@jwt_required
def test_update_success(request):
    """Test update success message"""
    update_success(request, 'Test Entity', 'Sample Item')
    return redirect('core:test_messages')


@jwt_required
def test_delete_success(request):
    """Test delete success message"""
    delete_success(request, 'Test Entity', 'Sample Item')
    return redirect('core:test_messages')


@jwt_required
def test_validation_error(request):
    """Test validation error message"""
    validation_error(request, "This field is required.")
    return redirect('core:test_messages')


@jwt_required
def test_permission_error(request):
    """Test permission error message"""
    permission_error(request, "delete this item")
    return redirect('core:test_messages')


@jwt_required
def test_not_found_error(request):
    """Test not found error message"""
    not_found_error(request, 'Test Entity')
    return redirect('core:test_messages')


@jwt_required
def test_bulk_operation_success(request):
    """Test bulk operation success message"""
    from .message_utils import bulk_operation_success
    bulk_operation_success(request, 'Client', 5, 'processed')
    return redirect('core:test_messages')


@jwt_required
def test_bulk_operation_error(request):
    """Test bulk operation error message"""
    from .message_utils import bulk_operation_error
    bulk_operation_error(request, 'Client', ['Invalid email', 'Missing phone'])
    return redirect('core:test_messages')


@jwt_required
def change_password(request):
    """Change password view"""
    if request.method == 'POST':
        form = PasswordChangeForm(request.user, request.POST)
        
        if form.is_valid():
            new_password = form.cleaned_data['new_password']
            request.user.set_password(new_password)
            request.user.save()
            
            messages.success(request, 'Password changed successfully! Please log in again with your new password.')
            return redirect('home')
        else:
            messages.error(request, 'Please correct the errors below.')
    else:
        form = PasswordChangeForm(request.user)

    context = {
        'form': form,
    }
    
    return render(request, 'core/change_password.html', context)




@csrf_exempt
@require_http_methods(["GET"])
def search_programs(request):
    """AJAX endpoint for searching programs with priority ordering"""
    if request.method == 'GET':
        search_term = request.GET.get('q', '').strip()
        
        if not search_term:
            # If no query, return all active programs (limit to 20 for performance)
            queryset = Program.objects.filter(
                status='active',
                is_archived=False,
                department__is_archived=False
            ).exclude(
                department__name__iexact='HASS'
            )
            
            # Apply program manager filtering for empty search too
            if request.user.is_authenticated and not request.user.is_superuser:
                try:
                    staff = request.user.staff_profile
                    if staff.is_program_manager():
                        assigned_programs = staff.get_assigned_programs()
                        queryset = queryset.filter(id__in=assigned_programs.values_list('id', flat=True))
                except:
                    pass
            
            programs = queryset[:20]
            programs_data = []
            for program in programs:
                programs_data.append({
                    'id': program.id,
                    'external_id': str(program.external_id),
                    'name': program.name,
                    'department': program.department.name,
                    'location': program.location,
                    'capacity': program.capacity_current,
                    'description': program.description or '',
                })
            return JsonResponse({'success': True, 'programs': programs_data})
        
        # Filter programs based on user's access level
        queryset = Program.objects.filter(
            status='active',
            is_archived=False,
            department__is_archived=False
        ).exclude(
            department__name__iexact='HASS'
        )
        
        # Apply program manager filtering
        if request.user.is_authenticated and not request.user.is_superuser:
            try:
                staff = request.user.staff_profile
                if staff.is_program_manager():
                    assigned_programs = staff.get_assigned_programs()
                    queryset = queryset.filter(id__in=assigned_programs.values_list('id', flat=True))
            except:
                pass
        
        # First get programs that start with the search term (highest priority)
        starts_with_programs = queryset.filter(
            Q(name__istartswith=search_term) |
            Q(department__name__istartswith=search_term) |
            Q(location__istartswith=search_term)
        ).select_related('department')
        
        # Then get programs that contain the search term anywhere (lower priority)
        contains_programs = queryset.filter(
            Q(name__icontains=search_term) |
            Q(department__name__icontains=search_term) |
            Q(location__icontains=search_term) |
            Q(description__icontains=search_term)
        ).exclude(
            Q(name__istartswith=search_term) |
            Q(department__name__istartswith=search_term) |
            Q(location__istartswith=search_term)
        ).select_related('department')
        
        # Combine and limit results
        programs = list(starts_with_programs) + list(contains_programs)
        programs = programs[:20]  # Limit to 20 results
        
        programs_data = []
        for program in programs:
            programs_data.append({
                'id': program.id,
                'external_id': str(program.external_id),
                'name': program.name,
                'department': program.department.name,
                'location': program.location,
                'capacity': program.capacity_current,
                'description': program.description or '',
            })
        
        return JsonResponse({'success': True, 'programs': programs_data})
    
    return JsonResponse({'error': 'Invalid request method'}, status=405)


@method_decorator(jwt_required, name='dispatch')
class EnrollmentCSVExportView(StaffAccessControlMixin, AnalystAccessMixin, ProgramManagerAccessMixin, ListView):
    """Export enrollments to CSV with filtering support"""
    model = ClientProgramEnrollment
    template_name = 'core/enrollments.html'
    
    def get_queryset(self):
        # Use the same filtering logic as EnrollmentListView
        base_queryset = super().get_queryset()
        # Default to only non-archived enrollments
        queryset = base_queryset.filter(is_archived=False)
        
        # Apply additional filters
        department_filter = self.request.GET.get('department', '')
        status_filter = self.request.GET.get('status', '')
        client_search = self.request.GET.get('client_search', '').strip()
        program_search = self.request.GET.get('program_search', '').strip()
        
        # Handle department filter first
        if department_filter:
            queryset = queryset.filter(program__department__name=department_filter)
        
        # Handle status filtering
        if status_filter:
            if status_filter == 'active_only':
                # Show only non-archived enrollments
                queryset = queryset.filter(is_archived=False)
            elif status_filter == 'pending':
                # Show enrollments with status='pending' and not archived
                queryset = queryset.filter(status='pending', is_archived=False)
            elif status_filter == 'active':
                # Show enrollments with status='active' and not archived
                queryset = queryset.filter(status='active', is_archived=False)
            elif status_filter == 'completed':
                # Show enrollments with status='completed' and not archived
                queryset = queryset.filter(status='completed', is_archived=False)
            elif status_filter == 'future':
                # Show future enrollments (start date in future and not archived)
                from django.utils import timezone
                queryset = queryset.filter(start_date__gt=timezone.now().date(), is_archived=False)
            elif status_filter == 'archived':
                # Show only archived enrollments
                queryset = base_queryset.filter(is_archived=True)
        
        if client_search:
            queryset = queryset.filter(
                Q(client__first_name__icontains=client_search) |
                Q(client__last_name__icontains=client_search) |
                Q(client__preferred_name__icontains=client_search) |
                Q(client__alias__icontains=client_search)
            )
        
        if program_search:
            queryset = queryset.filter(
                Q(program__name__icontains=program_search) |
                Q(program__department__name__icontains=program_search)
            )
        
        return queryset.order_by('-created_at')
    
    def get(self, request, *args, **kwargs):
        # Create CSV response
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="enrollments_export.csv"'
        
        # Create CSV writer
        writer = csv.writer(response)
        
        # Write header row
        writer.writerow([
            'Client Name',
            'Client ID',
            'Program Name',
            'Department',
            'Start Date',
            'End Date',
            'Status',
            'Notes',
            'Created By',
            'Created At',
            'Updated By',
            'Updated At',
            'Is Archived'
        ])
        
        # Get filtered enrollments
        enrollments = self.get_queryset()
        
        # Write data rows
        from django.utils import timezone
        today = timezone.now().date()
        
        for enrollment in enrollments:
            # Calculate status based on current date
            if today < enrollment.start_date:
                status = "Pending"
            elif enrollment.end_date:
                if today > enrollment.end_date:
                    status = "Completed"
                else:
                    status = "Active"
            else:
                status = "Active"
            
            writer.writerow([
                f"{enrollment.client.first_name} {enrollment.client.last_name}",
                enrollment.client.client_id,
                enrollment.program.name,
                enrollment.program.department.name,
                enrollment.start_date.strftime('%Y-%m-%d') if enrollment.start_date else '',
                enrollment.end_date.strftime('%Y-%m-%d') if enrollment.end_date else '',
                status,
                enrollment.notes or '',
                enrollment.created_by or '',
                enrollment.created_at.strftime('%Y-%m-%d %H:%M:%S') if enrollment.created_at else '',
                enrollment.updated_by or '',
                enrollment.updated_at.strftime('%Y-%m-%d %H:%M:%S') if enrollment.updated_at else '',
                'Yes' if enrollment.is_archived else 'No'
            ])
        
        return response


@method_decorator(jwt_required, name='dispatch')
class RestrictionCSVExportView(AnalystAccessMixin, ProgramManagerAccessMixin, ListView):
    """Export restrictions to CSV with filtering support"""
    model = ServiceRestriction
    template_name = 'core/restrictions.html'
    
    def get_queryset(self):
        # Use the same filtering logic as the restrictions view
        queryset = super().get_queryset()
        
        # Apply additional filters if any
        restriction_type_filter = self.request.GET.get('restriction_type', '')
        scope_filter = self.request.GET.get('scope', '')
        status_filter = self.request.GET.get('status', '')
        bill_168_filter = self.request.GET.get('bill_168', '')
        no_trespass_filter = self.request.GET.get('no_trespass', '')
        search_query = self.request.GET.get('search', '').strip()
        
        if restriction_type_filter:
            queryset = queryset.filter(restriction_type=restriction_type_filter)
        
        if scope_filter:
            queryset = queryset.filter(scope=scope_filter)
        
        if bill_168_filter:
            if bill_168_filter == 'true':
                queryset = queryset.filter(is_bill_168=True)
            elif bill_168_filter == 'false':
                queryset = queryset.filter(is_bill_168=False)
        
        if no_trespass_filter:
            if no_trespass_filter == 'true':
                queryset = queryset.filter(is_no_trespass=True)
            elif no_trespass_filter == 'false':
                queryset = queryset.filter(is_no_trespass=False)
        
        if status_filter:
            if status_filter == 'active':
                queryset = queryset.filter(end_date__isnull=True) | queryset.filter(end_date__gt=timezone.now().date())
            elif status_filter == 'expired':
                queryset = queryset.filter(end_date__lt=timezone.now().date())
        
        if search_query:
            queryset = queryset.filter(
                Q(client__first_name__icontains=search_query) |
                Q(client__last_name__icontains=search_query) |
                Q(notes__icontains=search_query)
            )
        
        return queryset.order_by('-created_at')
    
    def get(self, request, *args, **kwargs):
        # Create CSV response
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="restrictions_export.csv"'
        
        # Create CSV writer
        writer = csv.writer(response)
        
        # Write header row
        writer.writerow([
            'Client Name',
            'Client ID',
            'Restriction Type',
            'Scope',
            'Program',
            'Duration',
            'Start Date',
            'End Date',
            'Is Indefinite',
            'Status',
            'Notes',
            'Created By',
            'Created At',
            'Updated By',
            'Updated At'
        ])
        
        # Get filtered restrictions
        restrictions = self.get_queryset()
        
        # Write data rows
        for restriction in restrictions:
            # Determine status
            status = 'Active'
            if restriction.end_date and restriction.end_date < timezone.now().date():
                status = 'Expired'
            elif restriction.is_indefinite:
                status = 'Indefinite'
            
            writer.writerow([
                f"{restriction.client.first_name} {restriction.client.last_name}",
                restriction.client.client_id,
                restriction.get_restriction_type_display(),
                restriction.get_scope_display(),
                restriction.program.name if restriction.program else 'All Programs',
                restriction.get_duration_display() if not restriction.is_indefinite else 'Indefinite',
                restriction.start_date.strftime('%Y-%m-%d') if restriction.start_date else '',
                restriction.end_date.strftime('%Y-%m-%d') if restriction.end_date else '',
                'Yes' if restriction.is_indefinite else 'No',
                status,
                restriction.notes or '',
                restriction.created_by or '',
                restriction.created_at.strftime('%Y-%m-%d %H:%M:%S') if restriction.created_at else '',
                restriction.updated_by or '',
                restriction.updated_at.strftime('%Y-%m-%d %H:%M:%S') if restriction.updated_at else ''
            ])
        
        return response


def help_page(request):
    """Static Help page with grouped guidance for app features."""
    if not request.user.is_authenticated:
        return redirect('home')
    return render(request, 'core/help.html')


@login_required
@require_http_methods(["GET"])
def notifications_list(request):
    """Return notifications for the authenticated staff user."""
    notifications_qs = Notification.objects.for_user(request.user).select_related('staff')
    unread_count = notifications_qs.filter(is_read=False).count()

    per_page_param = request.GET.get('per_page', 20)
    try:
        per_page = max(5, min(int(per_page_param), 50))
    except (TypeError, ValueError):
        per_page = 20

    paginator = Paginator(notifications_qs, per_page)
    page_number = request.GET.get('page', 1)
    page_obj = paginator.get_page(page_number)

    notifications = [
        {
            'id': str(notification.external_id),
            'title': notification.title,
            'message': notification.message,
            'category': notification.category,
            'created_at': notification.created_at.isoformat(),
            'created_at_display': notification.created_at.strftime('%b %d, %Y %I:%M %p'),
            'is_read': notification.is_read,
            'read_at': notification.read_at.isoformat() if notification.read_at else None,
            'metadata': notification.metadata or {},
        }
        for notification in page_obj.object_list
    ]

    return JsonResponse({
        'success': True,
        'notifications': notifications,
        'unread_count': unread_count,
        'page': page_obj.number,
        'has_next': page_obj.has_next(),
        'has_previous': page_obj.has_previous(),
    })


@login_required
@require_http_methods(["POST"])
def notification_mark_read(request, notification_id):
    """Mark a single notification as read."""
    notification = get_object_or_404(
        Notification.objects.for_user(request.user),
        external_id=notification_id
    )
    notification.mark_read()
    return JsonResponse({'success': True})


@login_required
@require_http_methods(["POST"])
def notification_mark_unread(request, notification_id):
    """Mark a single notification as unread."""
    notification = get_object_or_404(
        Notification.objects.for_user(request.user),
        external_id=notification_id
    )
    notification.mark_unread()
    return JsonResponse({'success': True})


@login_required
@require_http_methods(["POST"])
def notification_mark_all_read(request):
    """Mark all notifications for the current user as read."""
    notifications_qs = Notification.objects.for_user(request.user).filter(is_read=False)
    now = timezone.now()
    notifications_qs.update(is_read=True, read_at=now, updated_at=now)
    return JsonResponse({'success': True})