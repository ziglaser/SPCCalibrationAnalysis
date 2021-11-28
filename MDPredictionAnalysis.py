from bs4 import BeautifulSoup
import requests
from datetime import date, timedelta
import csv
import time
from concurrent.futures import as_completed
from requests_futures import sessions


md_url = "https://www.spc.noaa.gov/products/md/"  # + year/mdXXXX.html
watch_url = "https://www.spc.noaa.gov/products/watch/"  # + year/wwXXXX.html

exp_year_range = (2013, 2020)

# read state keys text file into dict
with open('state_keys.txt', mode='r') as inp:
    reader = csv.reader(inp)
    state_abbrvs = {rows[1]:rows[0] for rows in reader}

# Will have to decide where threshold is 3-6 hours

def read_SPC_time(time):
    tlist = time.split()
    # ttuple = (hour, day month, year)
    hour = int(tlist[0][:2])
    if tlist[1] == "PM": hour += 12
    day_month = tlist[4] + " " + tlist[5]
    year = int(tlist[6])
    ttuple = (hour, day_month, year)
    return ttuple

def scrape_forecasts(year_range, url, dest_file, loc=True, date=True, prob=True):
    data = []

    with sessions.FuturesSession() as session:
        futures = [session.get(site) for site in sites]
        for future in as_completed(futures):
            resp = future.result()
            data.append(resp.json())

    return data

def parse_forecast():
    return None

# Weather Watch data
for year in range(exp_year_range[0], exp_year_range[1] + 1):
    with sessions.Session
    last_errored = False
    watch = 1
    while not last_errored:
        print(f"{watch:04d}")
        r = requests.get("https://www.spc.noaa.gov/products/watch/2013/ww" + f"{watch:04d}" + ".html")
        soup = BeautifulSoup(r.content, 'html.parser')
        body = soup.get_text().upper()
        if "Not Found" in body:
            last_errored = True
            continue

        loc_target = "FOR PORTIONS OF"
        loc_start_index = body.find(loc_target) + len(loc_target)
        loc_end_index = body.find("EFFECTIVE")
        loc = body[loc_start_index:loc_end_index]
        states = [a for a in state_abbrvs.values() if a in loc]
        if not states:
            states = [state_abbrvs[a] for a in state_abbrvs.keys() if (" " + a) in loc]

        time_target = "NWS STORM PREDICTION CENTER NORMAN OK"
        time_start_index = body.find(time_target) + len(time_target) + 4
        time_end_index = body.find(loc_target)
        hour = read_SPC_time(body[time_start_index:time_end_index])

        with open("WWSamples.txt", 'a') as f:
            f.write(";".join([str(hour), str(states)]) + "\n")

        watch += 1

# MD Data
for year in range(exp_year_range[0], exp_year_range[1] + 1):
    last_errored = False
    discussion = 1
    while not last_errored:
        print(f"{discussion:04d}")
        r = requests.get("https://www.spc.noaa.gov/products/md/2013/md" + f"{discussion:04d}" + ".html")
        soup = BeautifulSoup(r.content, 'html.parser')
        body = soup.get_text().upper()
        if "Not Found" in body:
            last_errored = True
            continue
        prob_target = "PROBABILITY OF WATCH ISSUANCE..."
        if body.find(prob_target) >= 0:
            prob_index = body.find(prob_target) + len(prob_target)
            prob = body[prob_index: prob_index + 3].split()[0]

            loc_target = "AREAS AFFECTED..."
            loc_start_index = body.find(loc_target) + len(loc_target)
            loc_end_index = body.find("CONCERNING")
            loc = body[loc_start_index:loc_end_index]
            states = [a for a in state_abbrvs.values() if a in loc]
            if not states:
                states = [state_abbrvs[a] for a in state_abbrvs.keys() if (" " + a) in loc]

            time_target = "NWS STORM PREDICTION CENTER NORMAN OK"
            time_start_index = body.find(time_target) + len(time_target) + 4
            time_end_index = body.find(loc_target)
            hour = read_SPC_time(body[time_start_index:time_end_index])
            with open("MDSamples.txt", 'a') as f:
                f.write(";".join([str(hour), str(states), str(prob)]) + "\n")
        discussion += 1


