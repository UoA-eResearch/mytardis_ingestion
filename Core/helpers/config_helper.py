# config_helper.py
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
# Functions to process config files and return a dictionary based on
# list of keys

from decouple import Config, RepositoryEnv

local_keys = {
    'server': 'MYTARDIS_URL',
    'ingest_user': 'MYTARDIS_INGEST_USER',
    'ingest_api_key': 'MYTARDIS_INGEST_API_KEY',
    'facility_manager': 'MYTARDIS_FACILITY_MANAGER',
    'verify_certificate': 'MYTARDIS_VERIFY_CERT',
    'storage_box': 'MYTARDIS_STORAGE_BOX',
    'remote_root': 'FILEHANDLER_REMOTE_ROOT',
    'staging_root': 'FILEHANDLER_STAGING_ROOT',
    'origin': 'FILEHANDLER_ORIGIN_ROOT',
    'bucket': 'FILEHANDLER_S3_BUCKET',
    'endpoint_url': 'FILEHANDLER_S3_ENDPOINT_URL',
    'threshold': 'FILEHANDLER_S3_THRESHOLD',
    'blocksize': 'FILEHANDLER_BLOCKSIZE',
    'proxy_http': 'PROXY_HTTP',
    'proxy_https': 'PROXY_HTTPS',
    }

global_keys = {
    'raid_api_key': 'RAID_API_KEY',
    'raid_url': 'RAID_URL',
    'raid_cert': 'RAID_VERIFY_CERT',
    'ldap_url': 'LDAP_URL',
    'ldap_user_attr_map': 'LDAP_USER_ATTR_MAP_FIRST_NAME',
    'ldap_firstname': 'LDAP_USER_ATTR_MAP_LAST_NAME',
    'ldap_email': 'LDAP_USER_ATTR_MAP_EMAIL',
    'ldap_username': 'LDAP_USER_ATTR_MAP_UPI',
    'ldap_admin_user': 'LDAP_ADMIN_USER',
    'ldap_admin_password': 'LDAP_ADMIN_PASSWORD',
    'ldap_user_base': 'LDAP_USER_BASE',
    }

def process_config(keys = None,
                   local_filepath = None,
                   global_filepath = None):
    local_dict = {}
    global_dict = {}
    if not keys:
        keys = list(local_keys.keys())
        keys += list(global_keys.keys())
    if not local_filepath:
        if not global_filepath:
            for key in keys:
                if key in local_keys.keys():
                    local_dict[key] = None
    else:
        config = Config(RepositoryEnv(filepath))
        for key in keys:
            if key in local_keys.keys():
                local_dict[key] = config(local_keys[key],
                                         default=None)
    if not global_filepath:
        if not local_filepath:
            for key in keys:
                if key in global_keys.keys():
                    global_dict[key] = None
    else:
        config = Config(RepositoryEnv(filepath))
        for key in keys:
            if key in global_keys.keys():
                global_dict[key] = config(global_keys[key],
                                           default=None)
    return_dict = global_dict.update(local_dict)
    return return_dict
