import logging
from flask import request, redirect
from flask_login import login_user
from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.manager import AUTH_DB
from flask_appbuilder.security.views import AuthDBView
from flask_appbuilder import expose

log = logging.getLogger(__name__)

class HybridPKIAuthView(AuthDBView):
    """
    A custom view that intercepts the login page.
    If the PKI header is present, it logs the user in automatically (SSO).
    If it is not present, it renders the standard username/password form.
    """
    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        # 1. Check if the NGINX PKI header is present in the request
        client_dn = request.headers.get("Client-DN")
        
        if client_dn:
            # 2. Search for the user in the Airflow DB
            user = self.appbuilder.sm.find_user(username=client_dn)
            
            # 3. Just-In-Time (JIT) User Creation if they don't exist
            if not user:
                # --- Insert your Custom DynamoDB / Allowlist logic here ---
                # Example fallback logic:
                desired_role = "Viewer" 
                role_obj = self.appbuilder.sm.find_role(desired_role)
                
                if role_obj:
                    # Create the user programmatically
                    user = self.appbuilder.sm.add_user(
                        username=client_dn,
                        first_name="PKI",
                        last_name="User",
                        email=f"{client_dn}@pki.local", # A placeholder email
                        role=role_obj
                    )
                    log.info(f"JIT created new user for DN: {client_dn}")

            # 4. Log the user in and skip the password form
            if user:
                login_user(user)
                return redirect(self.appbuilder.get_url_for_index)

        # 5. Fallback: If no header is found (or JIT fails), render the DB password form
        return super().login()


class HybridSecurityManager(AirflowSecurityManager):
    # Override the default Database Authentication View with our Hybrid View
    authdbview = HybridPKIAuthView


# --- Standard FAB Configuration ---
# 1. Keep AUTH_DB so the underlying password verification system remains active
AUTH_TYPE = AUTH_DB 

# 2. Tell Airflow to use our custom security manager
SECURITY_MANAGER_CLASS = HybridSecurityManager
