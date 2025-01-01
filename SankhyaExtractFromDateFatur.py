from dotenv import load_dotenv
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
import logging
import json
import os

# Display information in the console.
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
            self.jsessionid = str(data_jsession["$"])  # AUTH
            logger.info("Login successful")
        except:
            logger.error("Login failed")

    def dbExplorer(self):
        # Execute the SQL query
        logger.info("Starting dbExplorer: Quering...")

        # Function to get the months
        def months_between(start_date, end_date):
            current_date = start_date
            while current_date < end_date:
                yield current_date
                current_date += relativedelta(months=1)

        start_date = date(2023, 1, 1)
        end_date = date(2024, 1, 1)

        # Loop for each month
        for month in months_between(start_date, end_date):
            data1 = month.strftime("%Y-%m-%d")
            next_month = month + relativedelta(months=1)
            data2 = next_month.strftime("%Y-%m-%d")

            # Reading the SQL file
            sql_query = f"""
            SELECT
                CAB.NUMNOTA as "nota",
                CAB.DTFATUR as "date",
                PRO.CODPROD as "cod_product",
                PRO.DESCRPROD as "product",
                PAR.CODPARC as "cod_client",
                VEN.APELIDO as "salesman",
                ITE.QTDNEG as "net_quantity"
            FROM TGFCAB CAB
            LEFT JOIN TGFPAR PAR ON CAB.CODPARC = PAR.CODPARC
            LEFT JOIN TGFVEN VEN ON PAR.CODVEND = VEN.CODVEND
            LEFT JOIN TGFITE ITE ON CAB.NUNOTA = ITE.NUNOTA
            LEFT JOIN TGFPRO PRO ON ITE.CODPROD = PRO.CODPROD
            LEFT JOIN TGFGRU GRU ON PRO.CODGRUPOPROD = GRU.CODGRUPOPROD
            WHERE CAB.TIPMOV = 'V'
                AND DTFATUR >= TO_DATE('{data1}', 'YYYY-MM-DD')
                AND DTFATUR <= TO_DATE('{data2}', 'YYYY-MM-DD')
            """

            # Headers
            headers = {"Cookie": f"JSESSIONID={self.jsessionid}"}

            # Parameters
            params = {
                "serviceName": "DbExplorerSP.executeQuery",
                "MAXRESULTSIZE": 10000,
                "requestBody": {"sql": sql_query},
            }

            try:
                # URL
                service_name = params["serviceName"]
                url = f"http://{self.host}:{self.port}/mge/service.sbr?serviceName={service_name}&outputType=json&mgeSession={self.jsessionid}"
                # Get response
                response = requests.get(url, json=params, headers=headers, timeout=30)
                # Get the data into a json file
                json_data = json.loads(response.content.decode("latin1"))
                # Concatenate
                columns = [
                    column["name"]
                    for column in json_data["responseBody"]["fieldsMetadata"]
                ]  # Extracting column names from the json response
                rows = json_data["responseBody"][
                    "rows"
                ]  # Extracting rows from the json response
                # Creating the dataframe with the rows and columns of the json response
                df = pd.DataFrame(
                    rows, columns=columns
                )  # Creating the dataframe with the rows and columns of the json response

            except requests.exceptions.RequestException as e:
                logger.error(f"dbExplorer failed: {e}")
                break
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON response: {e}")
                break
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")
                break

            # Format Data from dataframe: CHANGE HERE
            df["data"] = pd.to_datetime(df["data"], format="%d%m%Y %H:%M:%S")

            # Exporting
            dateNow = datetime.now().strftime("%Y%m%d_%H%M%S")
            df.to_csv(f"path-{data1}-{data2}-{dateNow}.csv", index=False, mode="w")

        logger.info("SQL query executed successfully")

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

    def getAlldata(self): ...


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
    sankhya.login()  # Open the connection
    sankhya.dbExplorer()  # Execute the SQL query
    sankhya.logout()  # Close the connection


if __name__ == "__main__":
    main()

# query_to_get_day_a_day = """
#     SELECT * FROM TGFITE
#     WHERE
#     EXTRACT(YEAR FROM DTALTER) = 2023
#     AND EXTRACT(MONTH FROM DTALTER) = 1
#     AND EXTRACT(DAY FROM DTALTER) = 23
# """
