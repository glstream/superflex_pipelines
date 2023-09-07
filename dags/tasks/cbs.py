import requests
from bs4 import BeautifulSoup
import re
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime


@task()
def cbs_web_scrapper() -> list:
    cbs_projections_players = []
    enrty_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    positions = ['QB', 'RB', 'WR', 'TE']
    scoring = 'ppr'
    for position in positions:
        base_projections = f"https://www.cbssports.com/fantasy/football/stats/{position}/2023/restofseason/projections/{scoring}/"

        res = requests.get(base_projections)
        soup = BeautifulSoup(res.text, "html.parser")

        projections = soup.find_all(
                    "tr", {"class": "TableBase-bodyTr"}
                )
        for k,v in enumerate(projections):
            player = projections[k].find_all('td')
            player_full_name = player[0].find_all('a')[-1].text.replace("'", "").replace('"', "").replace(" III", "").replace(" II", "").replace("Gabe", "Gabriel").replace(" Jr.", "")
            player_first_name = player_full_name.split()[0]
            player_last_name = player_full_name.split()[-1]
            total_projection = float(player[-2].text.lstrip().rstrip())
            cbs_projections_players.append([player_first_name, player_last_name,player_full_name, total_projection, enrty_time ])

        return cbs_projections_players


@task()
def data_validation(cbs_projections_players: list):
    return cbs_projections_players if len(cbs_projections_players) > 0 else False


@task()
def cbs_player_load(cbs_projections_players: list):

    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    execute_batch(
        cursor,
        """
            INSERT INTO dynastr.cbs_player_projections (
                player_first_name,
                player_last_name,
                player_full_name,
                total_projection,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (player_full_name)
        DO UPDATE SET player_first_name = EXCLUDED.player_first_name
            , player_last_name = EXCLUDED.player_last_name
            , player_full_name = EXCLUDED.player_full_name
            , total_projection = EXCLUDED.total_projection
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(cbs_projections_players),
        page_size=1000,
    )

    print(f"{len(cbs_projections_players)} cbs players to inserted or updated.")
    conn.commit()
    cursor.close()
    return "dynastr.cbs_player_projections"


@task()
def surrogate_key_formatting(table_name: str):
    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    print("Connection established")
    cursor.execute(
        f"""UPDATE {table_name} 
                        SET player_first_name = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(player_first_name,'.',''), ' Jr', ''), ' III',''),'Jeffery','Jeff'), 'Joshua','Josh'),'William','Will'), ' II', ''),'''',''),'Kenneth','Ken'),'Mitchell','Mitch'),'DWayne','Dee')
                        """
    )
    conn.commit()
    cursor.close()
    return

