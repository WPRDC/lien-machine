#import carto
from carto.auth import APIKeyAuthClient
from carto import sql as sql_thing

def user_authorize_carto():
    """Note that this differs from the organization-based authorization"""
    from carto_credentials import ORGANIZATION, USERNAME, API_KEY
    BASE_URL = "https://{user}.carto.com/". \
        format(organization=ORGANIZATION,
                       user=USERNAME)
    auth_client = APIKeyAuthClient(api_key=API_KEY, base_url=BASE_URL, organization=ORGANIZATION)
    return auth_client

auth_client = user_authorize_carto()

sql = sql_thing.SQLClient(auth_client)

query = "select * from {}".format(table_name)
r = sql.send(query)

delete_all = 'delete from {}'.format(table_name)
sql.send(delete_all)


insertions = []

for row in rows:
    insertion = "insert into {} ({}) VALUES ({})".format()
    insertions.append(insertion)

from sql_thing import BatchSQLClient

batchSQLClient = BatchSQLClient(auth_client)
createJob = batchSQLClient.create(insertions)

print(createJob['job_id'])

# Then you can keep checking until the job is done.

