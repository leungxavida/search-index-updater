from connector.base_connector import BaseConnector


class UserConnector(BaseConnector):
    def __init__(self):
        self.topic = 'dbsvname.public.accounts_user'

        self.index = 'users'
        self.doc_type = 'user'

        self.indexed_fields = {'id', 'uuid', 'last_login', 'created', 'modified', 'first_name', 'last_name',
                               'is_active', 'is_staff', 'date_joined', 'timezone', 'subscription_status',
                               'zip_code', 'cancel_date'}
