from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

TENANT_ID = "e074f948-369b-4e94-9e9f-043b7464a9db"
CLIENT_ID = "9729ce94-5a17-474e-89cd-7c0c2fab487d"
CLIENT_SECRET = "E~O7Q~vVn6JOsITujsEh3OStzo3P_2k4crUpu"

KEYVAULT_NAME = 'personal-nh'
KEYVAULT_URI = f'https://{KEYVAULT_NAME}.vault.azure.net/'

SERVER = 'nz-personal-nh.database.windows.net'
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

    return DB_USERNAME, DB_PASSWORD

def blob_connection_string():
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=weatherobservationdata;AccountKey=BqBTfTgRLh9df2dTAgjlNsBM6PlMO5pt/5H+dT0TB2gceX7ZXbxMbgvK6jqMl1bWIv+9sYzGgtWnU7Paz4GdAg==;EndpointSuffix=core.windows.net' #parser.parse_args().connection_string

    return connection_string