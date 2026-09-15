import logging
import boto3
import requests
from flask import request
from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.manager import AUTH_REMOTE_USER

log = logging.getLogger(__name__)

# Initialize boto3 outside the class so the connection pool is reused across requests
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
roles_table = dynamodb.Table('AirflowUserRolesMapping')

def get_role_from_dynamodb(dn_string):
    """Fetch the role from a DynamoDB table with Primary Key 'ClientDN'"""
    try:
        response = roles_table.get_item(Key={'ClientDN': dn_string})
        if 'Item' in response:
            return response['Item'].get('RoleName')
    except Exception as e:
        log.error(f"Error querying DynamoDB for role mapping: {e}")
    return None

def get_role_from_api(dn_string):
    """(Alternative) Fetch the role from an internal REST API"""
    try:
        # Example API call passing the DN as a query parameter
        response = requests.get(f"http://internal-auth-api.local/get-role?dn={dn_string}", timeout=3)
        if response.status_code == 200:
            return response.json().get('role')
    except Exception as e:
        log.error(f"Error querying Auth API: {e}")
    return None

class PKISecurityManager(AirflowSecurityManager):
    def auth_user_remote_user(self, username):
        # 1. Fetch the dynamic role from your external source
        desired_role_name = get_role_from_dynamodb(username)
        # desired_role_name = get_role_from_api(username) # Use this if using REST API
        
        # 2. Deny access if the external source doesn't recognize the user
        if not desired_role_name:
            log.warning(f"Login denied: DN '{username}' not found in external mapper.")
            return None

        # 3. Proceed with normal authentication
        user = super().auth_user_remote_user(username)
        
        if user:
            role_obj = self.find_role(desired_role_name)
            
            # Update user role if it has changed in the external system
            if role_obj and (len(user.roles) != 1 or user.roles[0].name != desired_role_name):
                user.roles = [role_obj]
                self.get_session.commit()
                log.info(f"Dynamically updated '{username}' to role '{desired_role_name}'.")
                
        return user

# FAB Configuration remains the same
AUTH_TYPE = AUTH_REMOTE_USER
AUTH_REMOTE_USER_ENV_VAR = "HTTP_CLIENT_DN"
AUTH_USER_REGISTRATION = True
SECURITY_MANAGER_CLASS = PKISecurityManager
