# accounts/views.py
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.contrib.auth import login, authenticate
from django.contrib import messages
from django.contrib.auth import get_user_model
from .forms import LoginForm, SignUpForm
from django.db.models import Q

User = get_user_model()

def user_signup(request):
    if request.user.is_authenticated:
        return redirect('dashboard:home')

    if request.method == 'POST':
        form = SignUpForm(request.POST)
        if form.is_valid():
            user = form.save(commit=False)
            user.username = form.cleaned_data['username']
            user.email = form.cleaned_data['email']
            user.save()
            messages.success(request, 'Account created successfully! You can now log in.')
            return redirect('accounts:login')
        else:
            messages.error(request, 'Please correct the errors below.')
    else:
        form = SignUpForm()
    return render(request, 'accounts/signup.html', {'form': form})


# def user_login(request):
#     if request.user.is_authenticated:
#         if request.user.is_superuser:
#             return redirect("adminpanel:admin_home")
#         else:
#             return redirect("dashboard:home")
#
#     if request.method == 'POST':
#         form = LoginForm(request, data=request.POST)
#         if form.is_valid():
#             username_or_email = form.cleaned_data.get('username')
#             password = form.cleaned_data.get('password')
#
#             # Allow login by either username or email
#             try:
#                 user = User.objects.get(Q(username=username_or_email) | Q(email=username_or_email))
#                 username = user.username
#             except User.DoesNotExist:
#                 username = username_or_email  # fallback
#
#             user = authenticate(request, username=username, password=password)
#             if user is not None:
#                 login(request, user)
#                 messages.success(request, f"Welcome, {user.username}!")
#                 return redirect('dashboard:home')
#             else:
#                 messages.error(request, "Invalid credentials. Please try again.")
#         else:
#             messages.error(request, "Invalid form submission.")
#     else:
#         form = LoginForm()
#     return render(request, 'accounts/login.html', {'form': form})


# accounts/views.py
from django.shortcuts import render, redirect
from django.contrib.auth import login, authenticate
from django.contrib import messages
from django.contrib.auth import get_user_model
from .forms import LoginForm, SignUpForm
from django.db.models import Q

User = get_user_model()

def user_signup(request):
    if request.user.is_authenticated:
        return redirect('dashboard:home')

    if request.method == 'POST':
        form = SignUpForm(request.POST)
        if form.is_valid():
            user = form.save(commit=False)
            user.username = form.cleaned_data['username']
            user.email = form.cleaned_data['email']
            user.save()
            messages.success(request, 'Account created successfully! You can now log in.')
            return redirect('accounts:login')
        else:
            messages.error(request, 'Please correct the errors below.')
    else:
        form = SignUpForm()
    return render(request, 'accounts/signup.html', {'form': form})


def user_login(request):
    # if already authenticated, route based on role
    if request.user.is_authenticated:
        if request.user.is_staff or request.user.is_superuser:
            return redirect("adminpanel:admin_home")
        else:
            return redirect("dashboard:home")

    if request.method == 'POST':
        form = LoginForm(request, data=request.POST)
        if form.is_valid():
            username_or_email = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')

            # Allow login by either username or email
            try:
                user = User.objects.get(Q(username=username_or_email) | Q(email=username_or_email))
                username = user.username
            except User.DoesNotExist:
                username = username_or_email  # fallback

            user = authenticate(request, username=username, password=password)
            if user is not None:
                login(request, user)
                messages.success(request, f"Welcome, {user.username}!")
                # redirect based on role
                if user.is_staff or user.is_superuser:
                    return redirect("adminpanel:admin_home")
                return redirect('dashboard:home')
            else:
                messages.error(request, "Invalid credentials. Please try again.")
        else:
            messages.error(request, "Invalid form submission.")
    else:
        form = LoginForm()
    return render(request, 'accounts/login.html', {'form': form})

def health_check(request):
    return JsonResponse({"status": "ok"})