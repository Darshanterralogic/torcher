"""
Inserts data from a Google sheet into a MongoDB database.

This script connects to a Google sheet, reads data from a specified sheet and inserts it into a MongoDB database.
The script requires the `pandas` and `pymongo` packages to be installed.

Example:
    python attendance_task.py

"""
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pymongo
import pandas as pd
import numpy as np
from pymongo import MongoClient


class PythonMongoDB:
    """
    A class for inserting data from a Google Sheet into a MongoDB database.
    """
    def __init__(self):
        self.sheet_id = '1KYP8EXjoCIh01aTreN5P8fiCDumCm4Zb5uv2oa6iLDQ'
        self.sheet_name = 'FNC_Attendance'
    @property
    def load_data_from_sheet(self):
        """
        Loads data from a Google Sheet.Returns dataframe in json format.
        """
        url = f'https://docs.google.com/spreadsheets/d/{self.sheet_id}/gviz/tq?tqx=out:csv&sheet={self.sheet_name}'

        data_frame = pd.read_csv(url)
        today = datetime.today()
        day_month = today.strftime('%d-%b')
        today_data=False
        for i in data_frame.columns:
            if day_month == i.split('.')[0]:
                today_data = True

        if today_data is not True:
            return False
        data_frame['day_month'] = day_month
        data_frame['Attendance'] = data_frame[day_month]
        data_frame['ID']=data_frame['Unnamed: 3']
        data_frame['Email']=data_frame['UPN ID']
        selected_columns = ['Name','ID','Email', 'Attendance','day_month']
        records_data=data_frame[selected_columns].to_dict(orient='records')
        records = []
        for i in records_data:
            if i['Attendance'] is np.nan:
                i['Attendance'] = None
            records.append(i)
        return records

    def insert_data_to_mongodb(self):
        """
        Inserts data from a Google Sheet into a MongoDB database.

        Returns:
            str: A message indicating whether the operation was successful or not.
        """
        try:
            data = self.load_data_from_sheet
            if data:
                client = MongoClient("mongodb://localhost:27017/")
                mydatabase = client["Attendance_DB"]
                mycollection = mydatabase["AttendanceTracker"]
                mycollection.insert_many(data)
                return True
            return 'No data Found'
        except pymongo.errors.PyMongoError as error_message:
            return str(error_message)
    def read_data_from_mongodb(self,input_string):
        """
        Read data from a MongoDB database.

        Returns:
            str: A message indicating whether the operation was successful or not.
        """
        try:
            client = MongoClient("mongodb://localhost:27017/")
            mydatabase = client["Attendance_DB"]
            mycollection = mydatabase["AttendanceTracker"]
            projection = {"_id": 0, "Name": 1,'ID':1,'Email':1, "Attendance": 1,"day_month":1}
            data =list(mycollection.find({"Attendance": input_string},projection))
            if data:
                data_frame = pd.DataFrame(data)
                print('How Many Employees are in '+input_string +' :',len(data_frame))

                return data_frame
            return 'No Data Found'


        except pymongo.errors.PyMongoError as error_message:
            return str(error_message)

    def get_not_filled_data_from_mongodb(self):
        """
        Read data from a MongoDB database.

        Returns:
            str: A message indicating whether the operation was successful or not.
                If success sent the emails to Not filled Employees
        """
        try:
            client = MongoClient("mongodb://localhost:27017  /")
            mydatabase = client["Attendance_DB"]
            mycollection = mydatabase["AttendanceTracker"]
            projection = {"_id": 0, "Name": 1,'ID':1,'Email':1, "Attendance": 1}
            data =list(mycollection.find({"Attendance": None},projection))
            if data:
                data_frame = pd.DataFrame(data)
                print('Not Filled Employees : ',len(data_frame))
                print(data_frame)
                not_filled_attendance_emails=  data_frame["Email"].dropna().tolist()
                #retuns the emails of not filled employees
                return not_filled_attendance_emails
            return 'No Data Found'
        except pymongo.errors.PyMongoError as error_message:
            return str(error_message)

if __name__ == "__main__":
    pm = PythonMongoDB()
    s=pm.insert_data_to_mongodb()
    if s is False:
        print(s)
    elif s is True:
        #Taking the input from the terminal for work type and getting data
        expected_input = ['WFO', 'WFH', 'PTO']
        while True:
            user_input = input("Enter Work Type of Employee: ")
            if user_input in expected_input:
                d = pm.read_data_from_mongodb(user_input)
                print(d)
                break
            print("Enter valid input like 'WFO','WFH','PTO'. Please try again.\n")
        #Getting Not Filled Employees data
        user_not_filled_input  = input("Do you want to get NOT FILLED EMPLOYEES DATA continue? (y/n): ")
        if user_not_filled_input.lower() == "y":
            # Getting Not Filled Employees emails as reponse
            response_not_filled_emails = pm.get_not_filled_data_from_mongodb()

            if len(response_not_filled_emails)>0:
                send_emails_permission = input("Do you want to SENT REMAINDER EMAILS continue? (y/n): ")
                if send_emails_permission.lower() == "y":
                    # list of email id's to send the email
                    #recipient_emails = response_not_filled_emails
                    recipient_emails = ['subramanyam.vegi@terralogic.com']
                    SENDER_EMAIL = "darshanumesh1994@gmail.com"
                    SENDER_PASSWORD = "lcxrcrxdqwkbmbha"

                    # Create the email message
                    message = MIMEMultipart()
                    message['From'] = SENDER_EMAIL
                    message['Subject'] = "Reminder: Update Attendance Sheet"
                    BODY = 'Dear Employee,\n\nI hope this email finds you well. I am writing to remind you to update your attendance for today.\nIt is important that we keep our records up to date.\n\nBest regards,\nSubramanyam'

                    message.attach(MIMEText(BODY, 'plain'))
                    try:
                        # Set up the SMTP server and send the emails
                        with smtplib.SMTP('smtp.gmail.com', 587) as smtp_server:
                            smtp_server.starttls()
                            smtp_server.login(SENDER_EMAIL, SENDER_PASSWORD)

                            for recipient_email in recipient_emails:
                                message['To'] = recipient_email
                                text = message.as_string()
                                smtp_server.sendmail(SENDER_EMAIL, recipient_email, text)
                            print('Emails sent successfully to not filled employees!')

                    except smtplib.SMTPException as e:
                        print('Error:', e)
                elif send_emails_permission.lower() == "n":
                    print("You choose to quit.")
                else:
                    print("Invalid response. Please enter 'y' or 'n'.")
            else:
                print("No Emails Found")
        elif user_not_filled_input.lower() == "n":
            print("You choose to quit.")
        else:
            print("Invalid response. Please enter 'y' or 'n'.")


