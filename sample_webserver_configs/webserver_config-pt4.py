import logging
from flask import request, redirect
from flask_login import login_user
from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.manager import AUTH_DB
from flask_appbuilder.security.views import AuthDBView
from flask_appbuilder import expose

log = logging.getLogger(__name__)

class HybridPKIAuthView(AuthDBView):
    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        # 1. If the admin is explicitly submitting the DB login form, let FAB handle it.
        # This prevents the SSO logic from hijacking the form submission if the 
        # admin happens to have a client certificate installed in their browser.
        if request.method == "POST" and "username" in request.form:
            return super().login()

        # 2. If the user hits the break-glass URL, skip SSO and render the form.
        if request.args.get("breakglass") == "true":
            return super().login()

        # 3. Standard SSO Flow
        client_dn = request.headers.get("Client-DN")
        if client_dn:
            user = self.appbuilder.sm.find_user(username=client_dn)
            
            # --- JIT / External Mapper Logic Goes Here ---
            
            if user:
                login_user(user)
                return redirect(self.appbuilder.get_url_for_index)

        # 4. Fallback (If no header is present, render the DB form anyway)
        return super().login()

class HybridSecurityManager(AirflowSecurityManager):
    authdbview = HybridPKIAuthView

AUTH_TYPE = AUTH_DB 
SECURITY_MANAGER_CLASS = HybridSecurityManager
