import re, sys, json, datetime, pprint, time
from marshmallow import fields, pre_load, post_load

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl')
import pipeline as pl
from subprocess import call
import pipe_raw_lien_records_from_csv as pipe

from parameters.local_parameters import SETTINGS_FILE

schema = pipe.RawLiensSchema
key_fields = pipe.key_fields
fields0 = schema().serialize_to_ckan_fields()
fields_to_publish = pipe.fields_to_publish
print("fields_to_publish = {}".format(fields_to_publish))

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    kwparams = dict(resource_name='Tax-lien satisfaction records to present (alpha)', schema=schema, key_fields=key_fields, server='production', pipe_name='sats_pipeline', fields_to_publish=fields_to_publish)
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
        pipe.main(target=target_file, **kwparams)
    else:
        raise ValueError('No target file specified.')
