# LDAP access functions

def get_user_from_upi(ldap_dict,
                      upi):
        server = ldap3.Server(ldap_dict['url'])
        search_filter = f'({ldap_dict["user_attr_map"]["upi"]}={upi})'
        with ldap3.Connection(server,
                              auto_bind=True,
                              user=ldap_dict['admin_user'],
                              password=ldap_dict['admin_password']) as connection:
            connection.search(ldap_dict['user_base'],
                              search_filter,
                              attributes=['*'])
        if len(connection.entries) > 1:
            error_message = f'More than one person with UPI: {upi} has been found in the LDAP'
            logger.error(error_message)
            raise Exception(error_message)
        elif len(connection.entries) == 0:
            error_message = f'No one with UPI: {upi} has been found in the LDAP'
            logger.error(error_message)
            return None
        else:
            person = connection.entries[0]
        username = person[ldap_dict['user_attr_map']['upi']].value
        first_name = person[ldap_dict['user_attr_map']['first_name']].value
        last_name = person[ldap_dict['user_attr_map']['last_name']].value
        email = person[ldap_dict['user_attr_map']['email']].value
        details = {'username': username,
                   'first_name': first_name,
                   'last_name': last_name,
                   'email': email}
        return details
