# LDAP access functions
import ldap3
import logging

logger = logging.getLogger(__name__)


def get_user_from_upi(ldap_dict,
                      upi):
    try:
        details = do_ldap_search(ldap_dict,
                                 'upi',
                                 upi)
    except Exception as err:
        raise err
    return details


def get_user_from_email(ldap_dict,
                        email):
    try:
        details = do_ldap_search(ldap_dict,
                                 'email',
                                 email)
    except Exception as err:
        raise err
    return details


def do_ldap_search(ldap_dict,
                   search_action,
                   value):
    if search_action not in ldap_dict['user_attr_map'].keys():
        error_message = f'{search_action} is not a valid key in the LDAP user attribute map'
        logger.error(error_message)
        raise KeyError(error_message)
    server = ldap3.Server(ldap_dict['url'])
    search_filter = f'({ldap_dict["user_attr_map"][search_action]}={value})'
    with ldap3.Connection(server,
                          auto_bind=True,
                          user=ldap_dict['admin_user'],
                          password=ldap_dict['admin_password']) as connection:
        connection.search(ldap_dict['user_base'],
                          search_filter,
                          attributes=['*'])
        if len(connection.entries) > 1:
            error_message = f'More than one person with {search_action}: {value} has been found in the LDAP'
            if logger:
                logger.error(error_message)
            raise Exception(error_message)
        elif len(connection.entries) == 0:
            error_message = f'No one with {search_action}: {value} has been found in the LDAP'
            if logger:
                logger.warning(error_message)
            return None
        else:
            person = connection.entries[0]
            username = person[ldap_dict['user_attr_map']['upi']].value
            first_name = person[ldap_dict['user_attr_map']['first_name']].value
            last_name = person[ldap_dict['user_attr_map']['last_name']].value
            try:
                email = person[ldap_dict['user_attr_map']['email']].value
            except KeyError:
                email = 'email'
            details = {'username': username,
                       'first_name': first_name,
                       'last_name': last_name,
                       'email': email}
            return details
