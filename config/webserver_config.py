import logging

from flask import request, redirect
from flask_login import login_user
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from flask_appbuilder.security.manager import AUTH_DB
from flask_appbuilder.security.views import AuthDBView
from flask_appbuilder import expose

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Role resolution
#
# ROLE_SOURCE = "static"  -> DN_ROLE_ALLOWLIST below (active today, no
#                             external dependency, good for local dev).
# ROLE_SOURCE = "dynamic" -> looks the role up in DynamoDB (falling back to
#                             an internal REST API). Flip this once the
#                             'AirflowUserRolesMapping' table / internal API
#                             is actually reachable from this container --
#                             it isn't in this local compose setup.
# ---------------------------------------------------------------------------
ROLE_SOURCE = "static"  # "static" | "dynamic"

# Keys are the expected Client DNs (from the mTLS client cert), values are
# the Airflow role to assign.
DN_ROLE_ALLOWLIST = {
    "CN=Alice,OU=Data-Engineering,O=MyCompany,C=US": "Admin",
    "CN=Bob,OU=Data-Science,O=MyCompany,C=US": "User",
    "CN=Charlie,OU=Operations,O=MyCompany,C=US": "Viewer",
}


def _resolve_role_static(client_dn):
    return DN_ROLE_ALLOWLIST.get(client_dn)


def _resolve_role_dynamic(client_dn):
    """Not wired up locally -- needs AWS creds/network access to the
    'AirflowUserRolesMapping' DynamoDB table or http://internal-auth-api.local
    from inside the container."""
    import boto3
    import requests

    try:
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.Table("AirflowUserRolesMapping")
        response = table.get_item(Key={"ClientDN": client_dn})
        if "Item" in response:
            return response["Item"].get("RoleName")
    except Exception as e:
        log.error(f"Error querying DynamoDB for role mapping: {e}")

    try:
        response = requests.get(
            f"http://internal-auth-api.local/get-role?dn={client_dn}", timeout=3
        )
        if response.status_code == 200:
            return response.json().get("role")
    except Exception as e:
        log.error(f"Error querying Auth API: {e}")

    return None


def resolve_role(client_dn):
    if ROLE_SOURCE == "dynamic":
        return _resolve_role_dynamic(client_dn)
    return _resolve_role_static(client_dn)


class HybridPKIAuthView(AuthDBView):
    """Intercepts /login/. If a Client-DN header is present (set by the
    mTLS-terminating proxy) and resolves to an allowed role, JIT-creates
    and logs the user in via SSO. Otherwise falls back to the normal
    Airflow username/password form."""

    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        # Let an in-flight password-form submission through untouched, so an
        # admin with a client cert installed can still use the DB login.
        if request.method == "POST" and "username" in request.form:
            return super().login()

        # Break-glass: ?breakglass=true skips SSO and renders the DB form.
        if request.args.get("breakglass") == "true":
            return super().login()

        client_dn = request.headers.get("Client-DN")
        if client_dn:
            desired_role_name = resolve_role(client_dn)

            if not desired_role_name:
                log.warning(f"Login denied: DN '{client_dn}' is not authorized.")
            else:
                user = self.appbuilder.sm.find_user(username=client_dn)

                if not user:
                    role_obj = self.appbuilder.sm.find_role(desired_role_name)
                    if role_obj:
                        user = self.appbuilder.sm.add_user(
                            username=client_dn,
                            first_name="PKI",
                            last_name="User",
                            email=f"{client_dn}@pki.local",
                            role=role_obj,
                        )
                        log.info(f"JIT created new user for DN: {client_dn}")
                elif len(user.roles) != 1 or user.roles[0].name != desired_role_name:
                    role_obj = self.appbuilder.sm.find_role(desired_role_name)
                    if role_obj:
                        user.roles = [role_obj]
                        self.appbuilder.sm.get_session.commit()
                        log.info(f"Updated '{client_dn}' to role '{desired_role_name}'.")

                if user:
                    login_user(user)
                    return redirect(self.appbuilder.get_url_for_index)

        # No header, or the DN wasn't authorized -- fall back to the DB form.
        return super().login()


class HybridSecurityManager(FabAirflowSecurityManagerOverride):
    authdbview = HybridPKIAuthView


AUTH_TYPE = AUTH_DB
SECURITY_MANAGER_CLASS = HybridSecurityManager
