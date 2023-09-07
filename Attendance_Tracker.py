def pthon_mongoDB():
    import pandas as pd
    SHEET_ID = '1ZsT28GlNYQwDoFK4AN_soPsHa9czL7f9EcP61W6cFP4'
    SHEET_NAME = 'FNC_Project_Member_Details'
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    df = pd.read_csv(url)
    data = df.to_dict('records')

    # importing module
    from pymongo import MongoClient

    # creation of MongoClient
    client = MongoClient()

    # Connect with the portnumber and host
    client = MongoClient("mongodb://localhost:27017/")

    # Access database
    mydatabase = client["Attendance_DB"]

    # Access collection of the database
    mycollection = mydatabase["AttendanceTracker"]

    # insert data
    mycollection.insert_many(data)

    return 'Done'


if __name__ == "__main__":
    print(pthon_mongoDB())
