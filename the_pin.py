import json
import requests
import time

def valid_pins(pin_candidates):
    pins = []
    for candidate in pin_candidates:
        URL = "https://data.wprdc.org/api/action/datastore_search?"
        URL += "resource_id=518b583f-7cc8-4f60-94d0-174cc98310dc&q="+candidate
        # Eventually, we might want to upgrade this query to something
        # more targeted, like
        # SELECT * FROM "518b583f-7cc8-4f60-94d0-174cc98310dc" WHERE "PARID" LIKE '0001G00043%'

        # Actually, an advantage to formatting the query as above is that
        # it will search both the PIN ("PARID") and Alternate ID fields.
        # To fully take advantage of this, modify the logic below.


        try:
            r = requests.get(URL)
        except: # If at first you don't succeed,
            time.sleep(0.1)
            r = requests.get(URL) # ... try, try again.
        if r.status_code != 200:
            r = requests.get(URL)
        if r.status_code == 200:
            records = json.loads(r.text)["result"]["records"]
            if len(records) == 1:
                if records[0]["PARID"] == candidate:
                    pins.append(candidate)
                elif records[0]["ALT_ID"] == candidate:
                    # If the PIN comes up in the ALT_ID column, translate
                    # it to the non-alternate ID.
                    pins.append(records[0]["PARID"].strip())
                # Else this is some kind of bogus match where the PIN
                # occurs somewhere else in the record
            elif len(records) > 0:
                #This should never happen.
                raise ValueError('There are two different records in the Property Asssessment database with the same PIN' + str(candidate))
    return pins
