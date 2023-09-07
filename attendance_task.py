"""
Inserts data from a Google sheet into a MongoDB database.

This script connects to a Google sheet, reads data from a specified sheet and inserts it into a MongoDB database.
The script requires the `pandas` and `pymongo` packages to be installed.

Example:
    python attendance_task.py

"""
import pymongo
import pandas as pd
from pymongo import MongoClient


class PythonMongoDB:
    """
    A class for inserting data from a Google Sheet into a MongoDB database.
    """
    def __init__(self):
        self.sheet_id = '1ZsT28GlNYQwDoFK4AN_soPsHa9czL7f9EcP61W6cFP4'
        self.sheet_name = 'FNC_Project_Member_Details'

    def load_data_from_sheet(self):
        """
        Loads data from a Google Sheet.Returns dataframe in json format.
        """
        url = f'https://docs.google.com/spreadsheets/d/{self.sheet_id}/gviz/tq?tqx=out:csv&sheet={self.sheet_name}'
        data_frame = pd.read_csv(url)
        return data_frame.to_dict('records')

    def insert_data_to_mongodb(self):
        """
        Inserts data from a Google Sheet into a MongoDB database.

        Returns:
            str: A message indicating whether the operation was successful or not.
        """
        try:
            data = self.load_data_from_sheet()
            client = MongoClient("mongodb://localhost:27017/")
            mydatabase = client["Attendance_DB"]
            mycollection = mydatabase["AttendanceTracker"]
            mycollection.insert_many(data)
            return 'Done'
        except pymongo.errors.PyMongoError as error_message:
            return str(error_message)


if __name__ == "__main__":
    pm = PythonMongoDB()
    print(pm.insert_data_to_mongodb())
