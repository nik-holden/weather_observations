from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

TENANT_ID = "e074f948-369b-4e94-9e9f-043b7464a9db"
CLIENT_ID = "9729ce94-5a17-474e-89cd-7c0c2fab487d"
CLIENT_SECRET = "yoz8Q~FPoWiRWAVt0SSH_SLP1E2_EWwEOUNlBadF"

KEYVAULT_NAME = 'personal-nh'
KEYVAULT_URI = f'https://{KEYVAULT_NAME}.vault.azure.net/'

SERVER = 'personal-nh.database.windows.net'
DATABASE = 'general-data-collection'
AZURE_SQL_DRIVER = 'ODBC Driver 17 for SQL Server'

def db_credentials():
    _credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    _sc = SecretClient(vault_url=KEYVAULT_URI, credential=_credential)

    DB_USERNAME = _sc.get_secret("nh-per-db-un").value
    DB_PASSWORD = _sc.get_secret("nh-per-db-pw").value

    return DB_USERNAME, DB_PASSWORD, CLIENT_SECRET

def blob_connection_string():
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=weatherobservationdata;AccountKey=BqBTfTgRLh9df2dTAgjlNsBM6PlMO5pt/5H+dT0TB2gceX7ZXbxMbgvK6jqMl1bWIv+9sYzGgtWnU7Paz4GdAg==;EndpointSuffix=core.windows.net'

    return connection_string


def _blob_connection_string():
    account_name = 'weatherobssea'
    account_key = 'QjcolMTyXYBSMWellRBlT5TOliVPwmSYieubmVYtqJ5VX5taDaO/HXCp37TiSLA2qwi+r21UAIq3+AStdxgmrQ=='
    endpoint_suffix = 'core.windows.net'

    _connection_string = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix={endpoint_suffix}'

    return _connection_string