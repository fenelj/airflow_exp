import os
from flask import request
from flask_login import login_user, current_user
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from flask_appbuilder.security.manager import AUTH_DB

# Use AUTH_DB to ensure the standard login screen is always available as a fallback
AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public" 

class CustomSecurityManager(FabAirflowSecurityManagerOverride):
    def __init__(self, appbuilder):
        super().__init__(appbuilder)
        
        # Hook into every Flask request before it is processed
        @appbuilder.get_app.before_request
        def auto_login_from_header():
            # If the user is already authenticated and has a session, we can skip
            # unless we specifically want to sync roles on every single request.
            # For SSO, syncing roles every time the gateway passes them ensures they stay up to date.
            
            username = request.headers.get("X-Forwarded-User")
            
            # If the gateway didn't pass a user, do nothing!
            # The user will naturally fall back to the standard Airflow login screen.
            if not username:
                return 
                
            # 1. Find or create the user dynamically
            user = self.find_user(username=username)
            if not user:
                user = self.add_user(
                    username=username,
                    first_name=username,
                    last_name="SSO",
                    email=f"{username}@sso.local",
                    role=self.find_role(AUTH_USER_REGISTRATION_ROLE)
                )
            
            # 2. Extract and assign roles dynamically
            roles_header = request.headers.get("X-Forwarded-Roles", "")
            if roles_header:
                roles = [r.strip() for r in roles_header.split(",") if r.strip()]
                
                user_roles = []
                for role_name in roles:
                    role = self.find_role(role_name)
                    if role:
                        user_roles.append(role)
                
                if user_roles:
                    user.roles = user_roles
                    self.get_session.commit()
            
            # 3. Log the user into Flask-Login seamlessly, bypassing the login screen
            if not current_user.is_authenticated or current_user.username != username:
                login_user(user)

SECURITY_MANAGER_CLASS = CustomSecurityManager
