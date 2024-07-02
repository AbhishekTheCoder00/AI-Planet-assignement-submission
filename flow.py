from metaflow import FlowSpec, step
import pandas as pd
from sqlalchemy import create_engine

class ETLFlow(FlowSpec):

    @step
    def start(self):
        self.engine = create_engine('postgresql://user:pass@localhost:5432/airbnb')
        
        self.data = pd.read_csv('AB_NYC_2019.csv')
        
        self.next(self.load_data)

    @step
    def load_data(self):
        self.data.to_sql('nyc_airbnb', self.engine, if_exists='replace', index=False)
        self.next(self.extract_data)

    @step
    def extract_data(self):
        self.data = pd.read_sql('SELECT * FROM nyc_airbnb', self.engine)
        self.next(self.transform_data)

    @step
    def transform_data(self):
        self.data['last_review_date'] = pd.to_datetime(self.data['last_review']).dt.date
        self.data['last_review_time'] = pd.to_datetime(self.data['last_review']).dt.time
        self.data.drop(columns=['last_review'], inplace=True)
        
        self.data['reviews_per_month'].fillna(0, inplace=True)
        self.data['last_review_date'].fillna(pd.Timestamp('today').date(), inplace=True)
        
        self.next(self.load_transformed_data)

    @step
    def load_transformed_data(self):
        self.data.to_sql('nyc_airbnb_transformed', self.engine, if_exists='replace', index=False)
        self.next(self.end)

    @step
    def end(self):
        print("ETL Pipeline Completed Successfully")

if __name__ == '__main__':
    ETLFlow()
