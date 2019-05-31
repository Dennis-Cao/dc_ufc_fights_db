import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import string
import re
import datetime
import sqlite3
import time
import os

all_links = []
location = []
events = []
f1 = []
f2 = []
winner = []
f1_odds = []
f2_odds = []
label = []
favourite = []

def scrape_data():
    # set up page to extract table
    data = requests.get("https://www.betmma.tips/mma_betting_favorites_vs_underdogs.php?Org=1")
    soup = BeautifulSoup(data.text, 'html.parser')

    # table with 98% width 
    table = soup.find('table', {'width': "98%"})
    # find all links in that table
    links = table.find_all('a', href=True)

    # append all links to a list 
    for link in links:
        all_links.append("https://www.betmma.tips/"+link.get('href'))

    # test for one use case
    for link in all_links:
        print(f"Now currently scraping link: {link}")

        data = requests.get(link)
        soup = BeautifulSoup(data.text, 'html.parser')
        time.sleep(1)
        # specific table with the information
        rows = soup.find_all('table', {'cellspacing': "5"})

        for row in rows:

            # check for draw, if draw, then skip
            # dictionary of won and lost
            odds = row.find_all('td', {'align': "center", 'valign': "middle"})
            # to avoid taking in draws
            if odds[0].text not in ['WON', 'LOST']:
                continue

            # event name
            h1 = soup.find("h1")
            # location and date
            h2 = soup.find("h2")
            
            events.append(h1.text)
            location.append(h2.text)

            odds_f1 = float(odds[2].text.strip(" @"))
            odds_f2 = float(odds[3].text.strip(" @"))

            f1_odds.append(odds_f1)
            f2_odds.append(odds_f2)

            # how to generate label
            odds_dict = {}
            odds_dict[odds[0].text] = odds_f1
            odds_dict[odds[1].text] = odds_f2 

            if odds_dict["WON"] > odds_dict["LOST"]:
                label.append("Underdog")
            else:
                label.append("Favourite")

            if odds_f1 > odds_f2:
                favourite.append("f2")
            else:
                favourite.append("f1")


            fighters = row.find_all('a', attrs={'href': re.compile("^fighter_profile.php")})
            f1.append(fighters[0].text)
            f2.append(fighters[1].text)
            winner.append(fighters[2].text)
    return None

def create_df():
    
    # creating dataframe
    df = pd.DataFrame()
    df["Events"] = events
    df["Location"] = location
    df["Fighter1"] = f1
    df["Fighter2"] = f2
    df["Winner"] = winner
    df["fighter1_odds"] = f1_odds
    df["fighter2_odds"] = f2_odds
    df["Favourite"] = favourite
    df["Label"] = label
    print(f"Successfully scraped {df.shape[0]} fights and last fight card was {df.iloc[-1, :]['Events']} {df.iloc[-1, :]['Location']}")
    print(df["Label"].value_counts()/len(df))
    
    return df

# functions to compute deltas

def odds_delta(df):
    if df["Favourite"] == "f1":
        return df["fighter1_odds"] - df["fighter2_odds"]
    else:
        return df["fighter2_odds"] - df["fighter1_odds"]

def reach_delta(df):
    if df["Favourite"] == "f1":
        return df["REACH_x"] - df["REACH_y"]
    else:
        return df["REACH_y"] - df["REACH_x"]

def slpm_delta(df):
    if df["Favourite"] == "f1":
        return df["SLPM_x"] - df["SLPM_y"]
    else:
        return df["SLPM_y"] - df["SLPM_x"]

def sapm_delta(df):
    if df["Favourite"] == "f1":
        return df["SAPM_x"] - df["SAPM_y"]
    else:
        return df["SAPM_y"] - df["SAPM_x"]
    
def stra_delta(df):
    if df["Favourite"] == "f1":
        return df["STRA_x"] - df["STRA_y"]
    else:
        return df["STRA_y"] - df["STRA_x"]
    
def strd_delta(df):
    if df["Favourite"] == "f1":
        return df["STRD_x"] - df["STRD_y"]
    else:
        return df["STRD_y"] - df["STRD_x"]
    
def td_delta(df):
    if df["Favourite"] == "f1":
        return df["TD_x"] - df["TD_y"]
    else:
        return df["TD_y"] - df["TD_x"]

def tda_delta(df):
    if df["Favourite"] == "f1":
        return df["TDA_x"] - df["TDA_y"]
    else:
        return df["TDA_y"] - df["TDA_x"]
    
def tdd_delta(df):
    if df["Favourite"] == "f1":
        return df["TDD_x"] - df["TDD_y"]
    else:
        return df["TDD_y"] - df["TDD_x"]

def suba_delta(df):
    if df["Favourite"] == "f1":
        return df["SUBA_x"] - df["SUBA_y"]
    else:
        return df["SUBA_y"] - df["SUBA_x"]

def age_delta(df):
    if df["Favourite"] == "f1":
        return df["Age_x"] - df["Age_y"]
    else:
        return df["Age_y"] - df["Age_x"]


def merge_data(df):
    
    # We're always asking for json because it's the easiest to deal with
    morph_api_url = "https://api.morph.io/Dennis-Cao/dc_ufc_fighters_db/data.json"

    # Keep this key secret using morph secret variables
    morph_api_key = os.environ['MORPH_API_KEY']

    r = requests.get(morph_api_url, params={
      'key': morph_api_key,
      'query': "select * from data"
    })

    j = r.json()
    
    # fighters db dataset to me merged
    fighters_db = pd.DataFrame.from_dict(j)
    
    test = pd.merge(df, fighters_db, left_on=["Fighter1"], right_on=["NAME"])
    test2 = pd.merge(test, fighters_db, left_on=["Fighter2"], right_on=["NAME"])
    
    test2["Odds_delta"] = test2.apply(odds_delta, axis=1)
    test2["REACH_delta"] = test2.apply(reach_delta, axis=1)
    test2["SLPM_delta"] = test2.apply(slpm_delta, axis=1)
    test2["SAPM_delta"] = test2.apply(sapm_delta, axis=1)
    test2["STRA_delta"] = test2.apply(stra_delta, axis=1)
    test2["STRD_delta"] = test2.apply(strd_delta, axis=1)
    test2["TD_delta"] = test2.apply(td_delta, axis=1)
    test2["TDA_delta"] = test2.apply(tda_delta, axis=1)
    test2["TDD_delta"] = test2.apply(tdd_delta, axis=1)
    test2["SUBA_delta"] = test2.apply(suba_delta, axis=1)
    test2["AGE_delta"] = test2.apply(age_delta, axis=1)
    
    final_df = test2[['Events', 'Location', 'Fighter1', 'Fighter2', 'Favourite', 'Label', 'REACH_delta', 'SLPM_delta', 'SAPM_delta', 'STRA_delta', 'STRD_delta', 'TD_delta', 'TDA_delta', 'TDD_delta', 'SUBA_delta', "AGE_delta", 'Odds_delta']]
    
    return final_df

scrape_data()
df = create_df()
df = merge_data(df)

conn = sqlite3.connect('data.sqlite')
df.to_sql('data', conn, if_exists='replace')
print('Fights Merged Db successfully constructed and saved')
conn.close()
