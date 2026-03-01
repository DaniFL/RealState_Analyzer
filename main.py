import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

class DataExtractor:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract(self):
        try:
            df = pd.read_csv(self.file_path, sep=";", encoding="latin1")
            return df
        except FileNotFoundError as e:
            print(f"Error al extraer datos: {e}")
            return None
    
class DataCleaner:
    def __init__(self, df):
        self.df = df

    def isnull(self):
        return self.df.isnull().sum()

    def drop_basic(self):
        # self.df = self.df.dropna(subset=["buy_price"])
        self.df = self.df.dropna(subset=["sq_mt_built"])
        self.df = self.df.drop_duplicates(keep="first")
        return self.df
    
    def fix_decimals(self):
        self.df["sq_mt_built"] = self.df["sq_mt_built"] / 10
        self.df["sq_mt_useful"] = self.df["sq_mt_useful"] / 10
        self.df["n_bathrooms"] = self.df["n_bathrooms"] / 10
        return self.df
    
    def clean_price(self):
        self.df["buy_price"] = self.df["buy_price"].str.replace("$", "", regex=False)
        self.df["buy_price"] = self.df["buy_price"].str.replace("€", "", regex=False)
        self.df["buy_price"] = self.df["buy_price"].str.replace(".", "", regex=False)
        self.df["buy_price"] = self.df["buy_price"].str.replace(",", ".", regex=False)
        self.df["buy_price"] = self.df["buy_price"].astype(float)
        return self.df
    
    def clean_neighborhood(self):
        regex = r"Neighborhood \d+: (?P<neighborhood>.*?) \((?P<sq_mt_price>[^\s]+).*?/m2\) - District (?P<district_number>\d+): (?P<district_name>.*)"        
        new_columns = self.df["neighborhood_id"].str.extract(regex)
        self.df = pd.concat([self.df, new_columns], axis=1)
        self.df["sq_mt_price"] = pd.to_numeric(self.df["sq_mt_price"], errors="coerce")
        self.df["district_number"] = pd.to_numeric(self.df["district_number"], errors="coerce")
        self.df.drop(columns=["neighborhood_id"], inplace=True)
        return self.df
    
    def clean_surface(self):
        self.df["sq_mt_useful"] = self.df["sq_mt_useful"].str.replace("m²", "", regex=False)
        self.df["sq_mt_useful"] = self.df["sq_mt_useful"].astype(float)
        return self.df
    
    def clean_basic(self):
        self.df = self.drop_basic()
        # self.df = self.clean_price()
        # self.df = self.clean_surface()
        self.df = self.fix_decimals()
        self.df = self.clean_neighborhood()
        return self.df

class DatabaseManager:
    def __init__(self, db_name):
        self.db_name = db_name
    
    def save_to_db(self, df, table_name):
        try:
            db_connection = sqlite3.connect(self.db_name)
            df.to_sql(table_name, db_connection, if_exists="replace", index=False)
            print(f"Datos guardados en la base de datos: {self.db_name}")
        except Exception as e:
            print(f"Error al guardar datos en la base de datos: {e}")

class DataAnalyzer:
    def __init__(self, df):
        self.df = df
    
    def analyze(self):
        self.df.groupby("district_name")["sq_mt_price"].mean().sort_values(ascending=False).plot(kind="bar")
        plt.show()

if __name__ == "__main__":
    extractor = DataExtractor("data/RealState_Madrid.csv")
    df = extractor.extract()
    print(df.head(5))
    cleaner = DataCleaner(df)
    df = cleaner.clean_basic()
    db_manager = DatabaseManager("real_estate.db")
    db_manager.save_to_db(df, "real_estate")
    analyzer = DataAnalyzer(df)
    analyzer.analyze()
