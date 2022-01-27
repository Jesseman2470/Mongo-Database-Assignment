from pymongo import MongoClient
import csv


def ingest_from_csv(filename, collection_name):
    try:
            with open(filename, newline='') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                entry = {}
                for idx, row in enumerate(reader):
                    if idx == 0:
                        continue
                    try:
                        entry = {
			    "Country": row[1],
                            "2010" : row[2],
                            "2011" : row[3],
                            "2012" : row[4],
                            "2013" : row[5],
                            "2014" : row[6],
                            "2015" : row[7],
                            "2016" : row[8],
         		    "2017" : row[9],
		            "2018" : row[10],
		            "2019" : row[11],

                        }

                        collection_name.insert_one(entry)

                    except:
                        return


def get_database():
    CONNECTION_STRING = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
    client = MongoClient(CONNECTION_STRING)
    return client['mydata']


if __name__ == "__main__":    
    dbname = get_database()
    collection_name = dbname['gpi_primary']
    ingest_from_csv('GPI\ source.csv', collection_name)