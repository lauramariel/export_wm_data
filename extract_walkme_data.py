"""
extract_walkme_data.py: Script to query mongodb for Test Drive module
completion data and push to google sheets

Author: laura@nutanix.com
Date:   2022-05-06
"""

import subprocess
import json
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials



dbsrv = "<DB_SRV>"
db = "<DB>"
collection = "<COLLECTION>"
username = "<USERNAME>"
password = "<PASSWORD>"



# todo - make this dynamic according to what month/year is passed to the script
exp_type = "prismpro"
month = 4
year = 2022
start_day = 1
end_day = 30

start_date = (datetime.datetime(year,month,start_day,0,0).timestamp())*1000 # 4/1/2022
end_date = (datetime.datetime(year,month,end_day,0,0).timestamp())*1000 # 4/30/2022

# construct query
query = '{"ctx_location_hostname": { "$not": { "$regex": "staging" }}, "oName": { "$regex": "'
query = query + exp_type
query = query + '"},"created_at": {"$gte":'
query = query + str(start_date)
query = query + ',"$lte":'
query = query + str(end_date)
query = query + '}}'

print(query)
fields = "created_at,user_email,oName,wm_env"
infile = "infile.json"
current_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
outfile = f"{month}-{year}.csv"

# generate data
with open(infile, "w+") as f:
    # run the call to get the data into the file
    subprocess.run(["mongoexport", dbsrv, "--db", db, "--collection", collection, "--username", username, "--password", password, "--query", query, "--type", "json", "--fields", fields], stdout=f)

# process data
csv_file = open(outfile, 'a')
dups = 0
non_zero_wm_env = 0
exp_completed = 0
last_line = ""

with open(infile, "r") as f:
    print("Processing records")
    for line in f:
        # convert line to dict
        line = json.loads(line)

        # get rid of the _id, we don't need it
        line.pop("_id")

        # make sure wm_env is 0, or move to the next
        if line.get("wm_env") != 0:
            non_zero_wm_env = non_zero_wm_env + 1
            continue
    
        if line.get("oName").split("|")[2] == "Experience Completed":
            exp_completed = exp_completed + 1
            continue

        # check if it is a duplicate of the last one we processed
        if last_line:
            if line == last_line:
                dups = dups + 1
                continue

        last_line = line
            
        module_name = line.get("oName").split("|")[2]
        timestamp = line.get("created_at")
        # convert time from epoch time in milliseconds
        date_time = datetime.datetime.fromtimestamp(timestamp/1000)
        user_email = line.get("user_email")

        csv_file.write(str(date_time) + "," + user_email + "," + module_name + "\n")

csv_file.close()

print(f"Removed {dups} duplicates")
print(f"Removed {exp_completed} 'Experience Completed'")
print(f"Removed {non_zero_wm_env} non-zero wm_env")

# put in Google Sheets

# scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
#          "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

# credentials = ServiceAccountCredentials.from_json_keyfile_name('secrets/testdrive01-d75667140466.json', scope)
# client = gspread.authorize(credentials)

# spreadsheet = client.open('Completed Prism Pro Modules')

# with open(outfile, 'r') as f:
#     content = f.read()
#     client.import_csv(spreadsheet.id, data=content)

        





