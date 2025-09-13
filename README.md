import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional, List
from pyspark.sql import SparkSession

os.environ['KAGGLE_USERNAME'] = 'anirudhrajagopal'
os.environ['KAGGLE_KEY'] = 'dcece31ed3e4801ce654f4bf81ffbcf2'

class Extract:
    def __init__(
        self,
        dataset: str = "sobhanmoosavi/us-accidents",
        extract_dir: str = r"C:\Users\user\Downloads\spark-4.0.0-bin-hadoop3\airflow_project\mysql_data",
        delete_zip_after_unzip: bool = True,
        encoding: str = "utf-8",
        verbose: bool = True,
    ):
        self.dataset = dataset
        self.extract_dir = Path(extract_dir)
        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self.delete_zip_after_unzip = delete_zip_after_unzip
        self.encoding = encoding
        self.verbose = verbose

        # 1) Check Kaggle CLI
        if shutil.which("kaggle") is None:
            raise EnvironmentError(
                "Kaggle CLI not found. Install with 'pip install kaggle' and ensure 'kaggle' is on PATH.\n"
            )

        # 2) If no CSV yet, download and unzip
        if not self._has_csv_in_dir():
            if self.verbose:
                print(f"Downloading {self.dataset} to: {self.extract_dir}")
            cmd = [
                "kaggle", "datasets", "download",
                "-d", self.dataset,
                "-p", str(self.extract_dir),
                "--unzip"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                raise RuntimeError(
                    "Kaggle download failed.\n"
                    f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}\n\n"
                    "Check Kaggle credentials and dataset slug."
                )

            if self.verbose:
                print("Download and unzip complete.")

            if self.delete_zip_after_unzip:
                for z in self.extract_dir.glob("*.zip"):
                    try:
                        z.unlink()
                    except Exception:
                        pass

        # 3) Locate the US Accidents CSV
        csv_path = self._find_accidents_csv()
        if not csv_path:
            raise FileNotFoundError(
                f"No US_Accidents*.csv found in {self.extract_dir}. "
                "Please verify the dataset contents."
            )

        self.csv_path = str(csv_path)
        if self.verbose:
            print(f"Using CSV: {self.csv_path}")

    def _has_csv_in_dir(self) -> bool:
        """
        Returns True if any CSV that looks like the US Accidents dataset exists in extract_dir.
        """
        patterns: List[str] = ["US_Accidents*.csv", "*.csv"]
        for pat in patterns:
            if any(self.extract_dir.glob(pat)):
                return True
        return False

    def _find_accidents_csv(self) -> Optional[Path]:
        """
        Returns a Path to the first CSV found (prefer US_Accidents*.csv).
        """
        preferred = sorted(self.extract_dir.glob("US_Accidents*.csv"))
        if preferred:
            # If multiple versions exist, pick the newest by name (rough heuristic)
            return preferred[-1]
        # Fallback to any CSV
        any_csv = sorted(self.extract_dir.glob("*.csv"))
        return any_csv[-1] if any_csv else None


if __name__ == "__main__":
    extract = Extract(
        dataset="sobhanmoosavi/us-accidents",
        extract_dir=r"C:\Users\user\Downloads\spark-4.0.0-bin-hadoop3\airflow_project\mysql_data",
        delete_zip_after_unzip=True,
        encoding="utf-8",
        verbose=True,
    )

    # Load the CSV with PySpark
    spark = SparkSession.builder.appName("USAccidentsAnalysis").getOrCreate()
    df = spark.read.option("header", True).csv(extract.csv_path)
    df.show(5)  # Show first 5 rows

    spark.stop()# adarshcode


import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import sys

os.environ['KAGGLE_USERNAME'] = 'anirudhrajagopal'
os.environ['KAGGLE_KEY'] = 'dcece31ed3e4801ce654f4bf81ffbcf2'

class Extract:
    def __init__(
        self,
        dataset: str = "sobhanmoosavi/us-accidents",
        extract_dir: str = r"C:\Users\user\Downloads\spark-4.0.0-bin-hadoop3\airflow_project\mysql_data",
        delete_zip_after_unzip: bool = True,
        encoding: str = "utf-8",
        verbose: bool = True,
    ):
        self.dataset = dataset
        self.extract_dir = Path(extract_dir)
        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self.delete_zip_after_unzip = delete_zip_after_unzip
        self.encoding = encoding
        self.verbose = verbose

        # 1) Check Kaggle CLI
        if shutil.which("kaggle") is None:
            raise EnvironmentError(
                "Kaggle CLI not found. Install with 'pip install kaggle' and ensure 'kaggle' is on PATH.\n"
            )

        # 2) If no CSV yet, download and unzip
        if not self._has_csv_in_dir():
            if self.verbose:
                print(f"Downloading {self.dataset} to: {self.extract_dir}")
            cmd = [
                "kaggle", "datasets", "download",
                "-d", self.dataset,
                "-p", str(self.extract_dir),
                "--unzip"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                raise RuntimeError(
                    "Kaggle download failed.\n"
                    f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}\n\n"
                    "Check Kaggle credentials and dataset slug."
                )

            if self.verbose:
                print("Download and unzip complete.")

            if self.delete_zip_after_unzip:
                for z in self.extract_dir.glob("*.zip"):
                    try:
                        z.unlink()
                    except Exception:
                        pass

        # 3) Locate the US Accidents CSV
        csv_path = self._find_accidents_csv()
        if not csv_path:
            raise FileNotFoundError(
                f"No US_Accidents*.csv found in {self.extract_dir}. "
                "Please verify the dataset contents."
            )

        self.csv_path = str(csv_path)
        if self.verbose:
            print(f"Using CSV: {self.csv_path}")

    def _has_csv_in_dir(self) -> bool:
        patterns: List[str] = ["US_Accidents*.csv", "*.csv"]
        for pat in patterns:
            if any(self.extract_dir.glob(pat)):
                return True
        return False

    def _find_accidents_csv(self) -> Optional[Path]:
        preferred = sorted(self.extract_dir.glob("US_Accidents*.csv"))
        if preferred:
            return preferred[-1]
        any_csv = sorted(self.extract_dir.glob("*.csv"))
        return any_csv[-1] if any_csv else None

def main(mysql_url, mysql_user, mysql_password):
    extract = Extract(
        dataset="sobhanmoosavi/us-accidents",
        extract_dir=r"C:\Users\user\Downloads\spark-4.0.0-bin-hadoop3\airflow_project\mysql_data",
        delete_zip_after_unzip=True,
        encoding="utf-8",
        verbose=True,
    )

    spark = SparkSession.builder.appName("YellowTaxiETL").getOrCreate()

    # Load CSV
    df = spark.read.option("header", True).csv(extract.csv_path)

    # Basic cleaning (adjust column names as needed for your dataset)
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn("tpep_pickup_datetime", to_date(col("tpep_pickup_datetime"))) \
               .withColumn("tpep_dropoff_datetime", to_date(col("tpep_dropoff_datetime")))

    # Example filter: only trips with fare > 0 (adjust column name if needed)
    if "fare_amount" in df.columns:
        df = df.filter(col("fare_amount") > 0)

    # Write to MySQL (Spark will create the table if it doesn't exist)
    df.write.format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "yellow_taxi_trips") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()

    spark.stop()

if __name__ == "__main__":
    # Usage: python etl_yellow_taxi.py <mysql_url> <mysql_user> <mysql_password>
    if len(sys.argv) != 4:
        print("Usage: python etl_yellow_taxi.py <mysql_url> <mysql_user> <mysql_password>")
        sys.exit(1)
    mysql_url = sys.argv[1]
    mysql_user = sys.argv[2]
    mysql_password = sys.argv[3]
    main(mysql_url, mysql_user, mysql_password)
###
#.env
# -------------------------
# Airflow Config
# -------------------------
AIRFLOW_UID=50000
AIRFLOW_GID=0


# Fernet key (use `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
AIRFLOW__CORE__FERNET_KEY=

# -------------------------
# Airflow Admin User
# -------------------------
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# -------------------------
# MySQL Config
# -------------------------
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=airflow
MYSQL_PASSWORD=airflow
MYSQL_DB=airflow_db
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=mysql+mysqlconnector://airflow:airflow@mysql:3306/airflow_db
###

pyspark
mysql-connector-python
sqlalchemy
