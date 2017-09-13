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

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    kwparams = dict(resource_name='Summary of liens to present (alpha)', schema=schema, key_fields=key_fields, server='production', pipe_name='liens_summary_pipeline', fields_to_publish=fields_to_publish, clear_first=True)
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
        pipe.main(target=target_file, **kwparams)
    else:
        raise ValueError('No target file specified.')
