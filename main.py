import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
from datetime import datetime
import logging
import os

logging.basicConfig(filename='datalog.log', filemode='a', format='%(asctime)s %(message)s')

watchDir = os.path.join('watchThisFolder')
DB_NAME = "dataDumper"
DB_USER = "root"
DB_PASSWORD = "Pavan@123"
HOSTNAME = '127.0.0.1'

flagTime = 0


class Watcher:
    DIRECTORY_TO_WATCH = watchDir

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()
        
import mysql.connector
from mysql.connector import errorcode

def create_database(cursor,cnx):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)

def insertintomysql(df):
    cnx = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD,
                              auth_plugin='mysql_native_password', host = HOSTNAME)
    cursor = cnx.cursor()
    TABLES = {}
    TABLES['data'] = ("CREATE TABLE `data` ("
            "  `id` int(11) NOT NULL AUTO_INCREMENT,"
            "  `S No` varchar(255),"
            "  `Level` varchar(255),"
            "  `C/E/ECR` varchar(255),"
            "  `BO/MIW/\nDRG/CHD/\nAssy/GA` varchar(255),"
            "  `Part Nos` varchar(255),"
            "  `Description` varchar(255),"
            "  `Qty` varchar(255),"
            "  `TARGET COST` varchar(255),"
            "  `BOM COST` varchar(255),"
            "  `GAP` varchar(255),"
            "  `AQF STATUS` varchar(255),"
            "  `PO Cost 1` varchar(255),"
            "  `SOB 1` varchar(255),"
            "  `Tax 1` varchar(255),"
            "  `Vendor Code 1` varchar(255),"
            "  `Supplier 1` varchar(255),"
            "  `PO Cost 2` varchar(255),"
            "  `SOB 2` varchar(255),"
            "  `Tax 2` varchar(255),"
            "  `Vendor Code 2` varchar(255),"
            "  `Supplier 2` varchar(255),"    
            "  `PO Cost 3` varchar(255),"
            "  `SOB 3` varchar(255),"
            "  `Tax 3` varchar(255),"
            "  `Vendor Code 3` varchar(255),"
            "  `Supplier 3` varchar(255),"    
            "  `PO Cost 4` varchar(255),"
            "  `SOB 4` varchar(255),"
            "  `Tax 4` varchar(255),"
            "  `Vendor Code 4` varchar(255),"
            "  `Supplier 4` varchar(255),"
            "  `PO Cost 5` varchar(255),"
            "  `SOB 5` varchar(255),"
            "  `Tax 5` varchar(255),"
            "  `Vendor Code 5` varchar(255),"
            "  `Supplier 5` varchar(255),"
            "  `Project` varchar(255),"
            "  `DollarPrice` varchar(255),"
            "  `ModificationTime` varchar(255),"
            "  PRIMARY KEY (`id`)"
            ") ENGINE=InnoDB")
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor,cnx)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    cols = "`,`".join([str(i) for i in df.columns.tolist()])
    # Insert DataFrame recrds one by one.
    for i,row in df.iterrows():
        sql = "INSERT INTO `data` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row))

        # the connection is not autocommitted by default, so we must commit to save our changes
    cnx.commit()
    cursor.close()
    cnx.close()


def helper(filePath):
    # try:

    df = pd.read_excel(filePath)   #load the file into a dataframe
    df = df.dropna(thresh = 1 )    #dropping all rows which dosent have any value
    projectName = df.columns[1]    #grab the projectName
    dollarPrice = df.columns[6]    #grab the dollarPrice
    newHeader = df.iloc[0]         #grab the first row for the header
    df = df[1:]                    #take the data less the header row
    df.columns = newHeader         #set the header row as the df header
    now = datetime.now()
    dtString = now.strftime("%d/%m/%Y %H:%M:%S")
    df=df.assign(Project= projectName)           #assigning projectName DollarPrice and ModificationTime
    df=df.assign(DollarPrice = dollarPrice)
    df = df.assign(ModificationTime = dtString)
    insertintomysql(df)

    # except:
    #     logging.error("Error in file")
    #     print("unable to insert")



class Handler(FileSystemEventHandler):

    @staticmethod

    def convertTime(s):
        return s.second + s.minute*60 + s.hour*3600

    def on_any_event(self,event):
        global flagTime
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            x = str(event.src_path)
            if(x[-1]=='x' or x[-1]=='s'):
            # Take any action here when a file is first created.
                print("Received created event - %s." % event.src_path)
                logging.warning("Created File")
        elif event.event_type == 'modified':
            x = str(event.src_path)
            # print(x)
            if( (x[-1]=='x' or x[-1]=='s' ) and ( flagTime == 0 or self.convertTime(datetime.now()) - flagTime > 5 ) ):
                helper(event.src_path)
            # Taken any action here when a file is modified.
                print("Received modified event - %s." % event.src_path)
                logging.warning("File has been modified")
                flagTime = self.convertTime(datetime.now())


w = Watcher()
w.run()