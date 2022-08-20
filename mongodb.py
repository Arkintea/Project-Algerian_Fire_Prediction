import pymongo
from app_log import log


class MongoDBManagement:

    def __init__(self, username, password):
        """
        This function sets the required url
        """
        try:
            self.username = username
            self.password = password
            self.url = f"mongodb+srv://{self.username}:{self.password}@testcluster.fjvlj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
            self.client = pymongo.MongoClient(self.url)

        except Exception as e:
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))


    def isDatabasePresent(self, db_name):
        '''Methods checks if Database already present or not'''
        try:
            if db_name in self.client.list_database_names():
                self.database = db_name
                return True
            else:
                return False
        except Exception as e:
            log.info("{db_name} already exists! Please chose different one" )


    def createDatabase(self, db_name):
        """This function create database if named database is not present"""
        try:
            self.database = self.client[str(db_name)]
            log.error('Database successfully created')
        except Exception as e:
            log.error('Database creation error occured', e)
            print('Database creation error occured')
        else:
            log.info(f"{db_name} already exists! Please chose different name" )
            print('Database name already exist')


    def isCollectionPresent(self, collection_name):
        """This checks if collection is present or not."""
        try:
            if self.database in self.client.list_database_names():
                if collection_name in self.database.list_collection_names():
                    log.info(f"Collection {collection_name} present")
                    return True
                else:
                    return False
            else:
                return False
        except Exception as e:
            log.info(f"[isCollectionPresent]: Failed to check collection\n" + str(e))

    
    def createCollection(self, collection_name):
        """Collection or table is created"""
        log.error('Create collection')
        
        #create collection/table
        try:
            self.collection = self.database[str(collection_name)]
            log.error('Collection created successfully')
        except Exception as e:
            log.error('Collection creation error occured', e)
            print('Collection creation error occured')
        else:
            log.info(f"{collection_name} already exists! Please chose different name" )
            print('Collection name already exists')


    def getRecords(self, collection_name):
        """This fetches collection data from database."""
        try:
            self.collection = self.database[collection_name]
            data = self.collection.find()
            log.info(f"Fetching records from collection")
            return data
        except Exception as e:
            log.info("[getRecords]:Problem occured while fetching data" + str(e))
            print("[getRecords]:Problem occured while fetching data")
    

    def insert(self, record):
        """clean the dataset and insert data into mongodb database"""
        log.info('execute the mongodb insertion function')
        try:
            #insert data into collection
            if type(record) == dict:
                self.collection.insert_one(record)
                log.error('Record inserted successfully')
            elif type(record) == list:
                self.collection.insert_many(record)
                log.error('Record inserted successfully')
        except Exception as e:
            log.error('Insertion operation error occured', e)
            print('Insertion operation error occured')
        else:
            print('Insertion operation successfully completed')
    
            
    def update(self, old_value, new_value):
        """Update data set with new values"""
        #update many operation
        try:
            self.collection.update_many(old_value, {'$set': new_value})
        except Exception as e:
            log.error('Update error', e)
            print('Update error')
        else:
            print('Update successfully completed')
    

    def delete(self, delete_data):
        """Delete operation on the dataset"""
        #delete many element of a collection based on some conditions
        try:
            self.collection.delete_many(delete_data)
        except Exception as e:
            log.error('Delete operation error occured', e)
            print('Delete operation error occured')
        else:
            print('Delete operation successfully completed')
    

    def find(self, query):
        """find a particular set of data"""
        return self.collection.find(query)

