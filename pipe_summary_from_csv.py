import re, sys, json, datetime, pprint, time
from marshmallow import fields, pre_load, post_load
sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl')
import pipeline as pl
from subprocess import call
import pipe_raw_lien_records_from_csv as pipe

from parameters.local_parameters import SETTINGS_FILE

class LiensSummarySchema(pl.BaseSchema):
    pin = fields.String(dump_to="pin", allow_none=True)
    number = fields.Integer(dump_to="number", allow_none=False)
    total_amount = fields.Float(dump_to="total_amount", allow_none=False)

    class Meta:
        ordered = True

schema = LiensSummarySchema
key_fields = ['pin'] # blocklot could also be included in the schema and key_fields.
fields0 = schema().serialize_to_ckan_fields()
fields_to_publish = fields0
print("fields_to_publish = {}".format(fields_to_publish))

def main(*args,**kwargs):
    server = kwargs.get('server','test-production')
    if server == 'production':
        kwparams = dict(resource_id='d1e80180-5b2e-4dab-8ec3-be621628649e', schema=schema, key_fields=key_fields, server=server, pipe_name='liens_summary_pipeline', fields_to_publish=fields_to_publish, clear_first=True)
    else:
        resource_name = 'Summary of liens to present (alpha)'
        #kwparams = dict(resource_name=resource_name, schema=schema, key_fields=key_fields, server=server, pipe_name='liens_summary_pipeline', fields_to_publish=fields_to_publish, clear_first = True) # Fails if the 
        # resource doesn't already exist. See inelegant solution below.
        kwparams = dict(resource_name=resource_name, schema=schema, key_fields=key_fields, server=server, pipe_name='liens_summary_pipeline', fields_to_publish=fields_to_publish)
    target_file = kwargs.get('input_file',None)
    if target_file is None:
        if len(sys.argv) > 1:
            target_file = sys.argv[1]
        else:
            raise ValueError("No target specified.")

    pipe.transmit(target=target_file, **kwparams) # This is a hack to get around the ETL framework's limitations. 1) Update (or create) the resource. 
    time.sleep(0.5)
    kwparams['clear_first']=True                  # Then...
    resource_id, original_url = pipe.transmit(target=target_file, **kwparams) # Clear the datastore and upload the data again.
    print("(Yes, this data is being deliberately piped to the CKAN resource twice. It has something to do with using the clear_first parameter to clear the datastore, which can only be done if the datastore has already been created, since the ETL framework is flawed.)")
    return resource_id, original_url

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
