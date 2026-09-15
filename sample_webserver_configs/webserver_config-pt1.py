import logging
from flask import request
from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.manager import AUTH_REMOTE_USER

log = logging.getLogger(__name__)

# 1. Define your Allowlist and Role Mapping
# Keys are the expected Client DNs, Values are the Airflow Roles
DN_ROLE_ALLOWLIST = {
    "CN=Alice,OU=Data-Engineering,O=MyCompany,C=US": "Admin",
    "CN=Bob,OU=Data-Science,O=MyCompany,C=US": "User",
    "CN=Charlie,OU=Operations,O=MyCompany,C=US": "Viewer",
}

class PKISecurityManager(AirflowSecurityManager):
    def auth_user_remote_user(self, username):
        # 2. Check the allowlist FIRST to prevent unauthorized account creation
        if username not in DN_ROLE_ALLOWLIST:
            log.warning(f"Login denied: DN '{username}' is not in the allowlist.")
            return None # Returning None blocks the login

        desired_role_name = DN_ROLE_ALLOWLIST[username]

        # 3. Proceed with JIT user creation/fetching via the base class
        user = super().auth_user_remote_user(username)
        
        if user:
            # 4. Fetch the role object and assign it
            role_obj = self.find_role(desired_role_name)
            
            if role_obj and (len(user.roles) != 1 or user.roles[0].name != desired_role_name):
                user.roles = [role_obj]
                self.get_session.commit()
                log.info(f"Assigned role '{desired_role_name}' to '{username}'.")
                
        return user

# FAB Configuration
AUTH_TYPE = AUTH_REMOTE_USER
AUTH_REMOTE_USER_ENV_VAR = "HTTP_CLIENT_DN"
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public" # Safe fallback, though our allowlist check prevents this from persisting
SECURITY_MANAGER_CLASS = PKISecurityManager
