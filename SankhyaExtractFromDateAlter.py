from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import pandas as pd
import requests
import glob
import logging
import json
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class SankhyaAPI:
    # Class to connect to Sankhya API
    def __init__(self, user: str, password: str, host: str, port: str):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.jsessionid = None

    def login(self):
        # Login to Sankhya API
        logger.info("Logging in to SankhyaAPI")

        params = {
            "serviceName": "MobileLoginSP.login",
            "requestBody": {
                "NOMUSU": {"$": self.user},
                "INTERNO": {"$": self.password},
            },
        }

        service_name = params["serviceName"]
        url = f"http://{self.host}:{self.port}/mge/service.sbr?serviceName={service_name}&outputType=json"

        try:
            response = requests.post(url, json=params, timeout=90)

            data = json.loads(response.content)

            data_jsession = data["responseBody"]["jsessionid"]

            self.jsessionid = str(data_jsession["$"])

            logger.info("Login successful")

        except requests.RequestException as e:
            logger.error(f"Login failed: {e}")

    def dbExplorer(self):
        logger.info("Executing SQL query")

        def query_each_day(year, month):
            # Get the number of days in the given month and year
            if month == 12:
                numero_de_dias_do_mes = (
                    datetime.date(year + 1, 1, 1) - datetime.date(year, month, 1)
                ).days
            else:
                numero_de_dias_do_mes = (
                    datetime.date(year, month + 1, 1) - datetime.date(year, month, 1)
                ).days

            # Loop through each day in the month
            for day in range(1, numero_de_dias_do_mes + 1):
                # Construct the query for the specific day
                query = f"""
                    SELECT
                        NUNOTA,
                        CODEMP,
                        CODPROD,
                        USOPROD,
                        QTDNEG,
                        QTDENTREGUE,
                        VLRUNIT,
                        VLRTOT,
                        VLRCUS,
                        VLRIPI,
                        ALIQIPI,
                        PRECOBASE,
                        CODVEND,
                        CUSTO
                        CODVOL,
                        STATUSNOTA,
                        DTALTER
                    FROM TGFITE
                    WHERE
                    EXTRACT(YEAR FROM "DTALTER") = {year}
                    AND EXTRACT(MONTH FROM "DTALTER") = {month}
                    AND EXTRACT(DAY FROM "DTALTER") = {day}
                """

                # Headers
                headers = {"Cookie": f"JSESSIONID={self.jsessionid}"}

                # Parameters
                params = {
                    "serviceName": "DbExplorerSP.executeQuery",
                    "MAXRESULTSIZE": 10000,
                    "requestBody": {"sql": query},
                }

                # URL
                service_name = params["serviceName"]
                url = f"http://{self.host}:{self.port}/mge/service.sbr?serviceName={service_name}&outputType=json&mgeSession={self.jsessionid}"

                # Get response
                response = requests.get(url, json=params, headers=headers, timeout=30)

                # Get the data into a json file
                json_data = json.loads(response.content.decode("latin1"))

                columns = [
                    column["name"]
                    for column in json_data["responseBody"]["fieldsMetadata"]
                ]  # Extracting column names from the json response
                rows = json_data["responseBody"][
                    "rows"
                ]  # Extracting rows from the json response

                # Creating the dataframe with the rows and columns of the json response
                df = pd.DataFrame(rows, columns=columns)

                # Format Data from dataframe: CHANGE HERE
                df["DTALTER"] = pd.to_datetime(df["DTALTER"], format="%d%m%Y %H:%M:%S")

                dateNow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

                df.to_csv(
                    f"./raw/Sankhya-Extract-{year}-{month}-{day}-{dateNow}.csv",
                    index=False,
                    mode="w",
                )

                logger.info("SQL query executed successfully")

        # Calling the function below will return the result between january through dezember each day
        for i in range(1, 13):
            query_each_day(2023, i)

    def logout(self):
        params = {
            "serviceName": "MobileLoginSP.logout",
            "status": "1",
            "pendingPrinting": "false",
        }

        # URL
        service_name = params["serviceName"]
        url = f"http://{self.host}:{self.port}/mge/service.sbr?serviceName={service_name}&outputType=json"
        # Post request
        requests.post(url, params=params)

        logger.info("Logout successful")

    def getAlldata(self, path, export_path):
        arquivos_csv = glob.glob(f"{path}*.csv")
        lista_df = []

        for arquivo in arquivos_csv:
            df = pd.read_csv(arquivo, delimiter=",", header=0)
            lista_df.append(df)

        # Concatena todos os DataFrames na lista em um único DataFrame
        df_final = pd.concat(lista_df, ignore_index=True)
        df_final.to_csv(f"{export_path}", index=False)


# Point Mount
def main():
    load_dotenv()

    # Login
    sankhya = SankhyaAPI(
        user=os.environ.get("SANKHYA_USER"),
        password=os.environ.get("SANKHYA_PASSWORD"),
        host=os.environ.get("SANKHYA_HOST"),
        port=os.environ.get("SANKHYA_PORT"),
    )

    # Steps to pull data from Sankhya service
    sankhya.login()
    sankhya.dbExplorer()
    sankhya.logout()
    sankhya.getAlldata("./raw/", "./files.csv")


if __name__ == "__main__":
    main()

# query_to_get_day_a_day = """
#     SELECT * FROM TGFITE
#     WHERE
#     EXTRACT(YEAR FROM DTALTER) = 2023
#     AND EXTRACT(MONTH FROM DTALTER) = 1
#     AND EXTRACT(DAY FROM DTALTER) = 23
# """
