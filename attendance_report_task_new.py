"""
Inserts data from a Google sheet into a MongoDB database.

This script connects to a Google sheet, reads data from a specified sheet and inserts it into a MongoDB database.
The script requires the `pandas` and `pymongo` packages to be installed.

Example:
    python attendance_task.py

"""
from datetime import datetime
import math
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
        self.sheet_id = '1GPo47Wjggb3HgQtE_sqDXiTQSg1i3i-uoeFl8Lyh1Ao'
        self.sheet_name = 'FNC_Attendance'
    @property
    def load_data_from_sheet(self):
        """
        Loads data from a Google Sheet.Returns dataframe in json format.
        """
        url = f'https://docs.google.com/spreadsheets/d/{self.sheet_id}/gviz/tq?tqx=out:csv&sheet={self.sheet_name}'

        data_frame = pd.read_csv(url)
        today = datetime.today()
        today = today.strftime('%d-%b-%Y')
       # today = '31-Mar-2023'
        today_data=False
        for todate in data_frame.columns:
            if today == todate:
                today_data = True
        if today_data is not True:
            return False
        data_frame['Attendance_date'] = today
        data_frame['Attendance'] = data_frame[today]
        data_frame['ID']=data_frame['Unnamed: 3']
        data_frame['Email']=data_frame['UPN ID']
        selected_columns = ['Name','ID','Email', 'Attendance','Attendance_date']
        records_data=data_frame[selected_columns].to_dict(orient='records')
        records = []
        for rec in records_data:
            if rec['Attendance'] is np.nan:
                rec['Attendance'] = None
            if rec['ID'] is np.nan:
                rec['ID'] = None
            if rec['Name'] is np.nan:
                rec['ID'] = None
            if rec['Email'] is np.nan:
                rec['Email'] = None
            records.append(rec)
        return records

    def insert_data_to_mongodb(self):
        """
        Inserts data from a Google Sheet into a MongoDB database.

        Returns:
            str: A message indicating whether the operation was successful or not.
        """
        try:
            data = self.load_data_from_sheet

            if data :
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
            today = datetime.today()
            today = today.strftime('%d-%b-%Y')
           # today = '31-Mar-2023'
            client = MongoClient("mongodb://localhost:27017/")
            mydatabase = client["Attendance_DB"]
            mycollection = mydatabase["AttendanceTracker"]
            projection = {"_id": 0, "Name": 1,'ID':1,'Email':1, "Attendance": 1,"Attendance_date":1}
            data =list(mycollection.find({"Attendance": input_string,"Attendance_date":today},projection))
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
            today = datetime.today()
            today = today.strftime('%d-%b-%Y')
           # today = '31-Mar-2023'
            client = MongoClient("mongodb://localhost:27017/")
            mydatabase = client["Attendance_DB"]
            mycollection = mydatabase["AttendanceTracker"]
            projection = {"_id": 0, "Name": 1,'ID':1,'Email':1, "Attendance": 1,'Attendance_date':1}
            data =list(mycollection.find({"Attendance": None,"Attendance_date":today},projection))
            if data:
                data_frame = pd.DataFrame(data)
                print('Not Filled Employees : ',len(data_frame))
                print(data_frame)
                not_filled_attendance_emails=  data_frame["Email"].dropna().tolist()
                #retuns the emails of not filled employees
                print(not_filled_attendance_emails)
                return not_filled_attendance_emails
            return 'No Data Found'
        except pymongo.errors.PyMongoError as error_message:
            return str(error_message)
    @property
    def get_sheet_data(self):
        """
        Loads data from a Google Sheet.Returns dataframe in json format.
        """
        url = f'https://docs.google.com/spreadsheets/d/{self.sheet_id}/gviz/tq?tqx=out:csv&sheet={self.sheet_name}'

        data_frame = pd.read_csv(url)
        today = datetime.today()
        today = today.strftime('%d-%b-%Y')
       # today = '31-Mar-2023'
        original_list=data_frame.columns
        # Get the index of today's date header in the list
        index_of_today = list(data_frame.columns).index(today)
        return {'original_list':original_list,'index_of_today':index_of_today,
                'total_data':data_frame}

    def read_week_data_from_mongodb(self,input_week):
        """
        Read data from a MongoDB database based on previous weeks.

        Returns:
            str: A message indicating whether the operation was successful or not.
        """
        try:
            res=self.get_sheet_data
            # Get the date headers for the previous week days
            previous_headers = ['Name','ID','Email']
            for index in range(1, input_week*8):
                previous_headers.append(res['original_list'][res['index_of_today'] - index])
            res_df=res['total_data']
            res_df['Name']=res['total_data']['Name']
            res_df['ID']=res['total_data']['Unnamed: 3']
            res_df['Email']=res['total_data']['UPN ID']


            today = datetime.today()
            today = today.strftime('%d-%b-%Y')
            today = '31-Mar-2023'
            client = MongoClient("mongodb://localhost:27017/")
            mydatabase = client["Attendance_DB"]
            mycollection = mydatabase["AttendanceTracker"]
            projection = {"Email": 1}
            today_not_filled_users = list(mycollection.find({"Attendance": None,"Attendance_date":today}, projection))
            if today_not_filled_users:
                email_ids=[]
                for email in today_not_filled_users:
                    if email['Email']:
                        email_ids.append(email['Email'])
                dframe = pd.DataFrame(res_df[previous_headers])
                #subset = dframe[dframe['Email'].isin(email_ids)]
                return dframe

            return 'No Data Found'
        except pymongo.errors.PyMongoError as error_message:
            return str(error_message)

    def get_emp_ids_with_no_emails(self):
        """
        Read data from a sheet with no email ids.

        Returns:
            str: A message indicating whether the operation was successful or not and emails data.
        """
        try:
            res=self.get_sheet_data
            res_df = res['total_data']
            res_df['Name'] = res['total_data']['Name']
            res_df['ID'] = res['total_data']['Unnamed: 3']
            res_df['Email'] = res['total_data']['UPN ID']
            selected_columns = ['Name', 'ID']
            records_data = res_df[res_df['Email'].isna()][selected_columns].to_dict(orient='records')
            if records_data:
                final_records=[]
                for item in records_data:
                    if math.isnan(item['ID']):
                        item['ID'] = None
                    else:
                        item['ID'] = int(item['ID'])
                    final_records.append(item)
                return final_records
            return False
        except Exception as error_message:
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
                    print('emails of not filled employees:',response_not_filled_emails)
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
        #Getting last week data of employees those who are not filled today
        user_not_filled_input = input("Do you want to get NOT FILLED EMPLOYEES WEEK DATA continue? (y/n): ")
        if user_not_filled_input.lower() == "y":
            while True:
                expected_weeks_input=['1','2']
                user_input_week = input("Enter how many weeks data you want 1 or 2 : ")
                if user_input_week in expected_weeks_input:
                    week_data = pm.read_week_data_from_mongodb(int(user_input_week))
                    print(week_data)
                    break
                print("Enter valid input like 1 or 2. Please try again.Press 0 for exit:\n")
        elif user_not_filled_input.lower() == "n":
            print("You choose to quit.")
        else:
            print("Invalid response. Please enter 'y' or 'n'.")
        #Getting Employee IDS who don't have email ids in the Attendance sheet
        user_no_email_input = input("Do you want to get Employee IDS who don't have emails in the Attendance sheet continue? (y/n): ")
        if user_no_email_input.lower() == "y":
            no_email_ids = pm.get_emp_ids_with_no_emails()

            if no_email_ids is False:
                print("No data found.")
            elif len(no_email_ids)>0:
                EMP_NAMES=""
                SLNO=1
                for i in no_email_ids:
                    e_Name=str(i['Name'])
                    EID = "None"
                    if i['ID']:
                        EID=str(i['ID'])
                    final_str=str(SLNO)+". Name: "+e_Name+' - ID: '+str(EID)+"\n"
                    EMP_NAMES+=final_str
                    SLNO+=1
                print(EMP_NAMES)
                send_emails_permission = input("Do you want to sent an email with who don't have emails in the Attendance sheet continue? (y/n): ")
                if send_emails_permission.lower() == "y":
                    recipient_emails = ['subramanyam.vegi@terralogic.com']
                    SENDER_EMAIL = "darshanumesh1994@gmail.com"
                    SENDER_PASSWORD = "lcxrcrxdqwkbmbha"

                    # Create the email message
                    message = MIMEMultipart()
                    message['From'] = SENDER_EMAIL
                    message['Subject'] = "Employees Missing Email Addresses in Attendance Sheet"
                    BODY = 'Dear Manager,\n\nI hope this email finds you well. I wanted to bring to your attention that while reviewing our attendance sheet, The following employees are missing email ' \
                           'addresses in their records:\n\n'+EMP_NAMES+'\n\n\tNOTE: This Auto Generated Email Please dont Reply Back.\n\nBest regards,\nAttendance Tracker Team.'

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
                            print('Emails sent successfully !!!')

                    except smtplib.SMTPException as e:
                        print('Error:', e)
                elif send_emails_permission.lower() == "n":
                    print("You choose to quit.")
                else:
                    print("Invalid response. Please enter 'y' or 'n'.")
            else:
                print("No Data Found")
        elif user_no_email_input.lower() == "n":
            print("You choose to quit.")
        else:
            print("Invalid response. Please enter 'y' or 'n'.")





