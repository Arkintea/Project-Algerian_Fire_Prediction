import pymongo
from app_log import log


class MongoDBManagement:

    def __init__(self, password):
        """
        This function sets the required url
        """
        try:
            self.url = f"mongodb+srv://assignment:{password}@cluster0.glme8.mongodb.net/?retryWrites=true&w=majority"
            self.client = pymongo.MongoClient(self.url)
        except Exception as e:
            raise Exception(f"(__init__): Something went wrong on initiation process\n" + str(e))


    def isDatabasePresent(self, db_name):
        '''Methods checks if Database already present or not'''
        try:
            if db_name in self.client.list_database_names():
                database = db_name
                return True
            else:
                return False
        except Exception as e:
            log.info("{db_name} already exists! Please chose different one" )


    def createDatabase(self, db_name):
        """This function create database if named database is not present"""
        try:
            database = self.client[str(db_name)]
            log.error('Database successfully created')
        except Exception as e:
            log.error('Database creation error occured', e)
            print('Database creation error occured')
        else:
            log.info(f"{db_name} already exists! Please chose different name" )
            print('Database name already exist')


    def isCollectionPresent(self, db_name, collection_name):
        """This checks if collection is present or not."""
        try:
            if collection_name in self.client[str(db_name)].list_collection_names():
                log.info(f"Collection {collection_name} present")
                return True
            else:
                return False
        except Exception as e:
            log.info(f"[isCollectionPresent]: Failed to check collection\n" + str(e))

    
    def createCollection(self, db_name, collection_name):
        """Collection or table is created"""
        log.error('Create collection')
        try:
            #create collection/table
            collection = self.client[str(db_name)][str(collection_name)]
            log.error('Collection created successfully')
        except Exception as e:
            log.error('Collection creation error occured', e)
            print('Collection creation error occured')
        else:
            log.info(f"{collection_name} already exists! Please chose different name" )
            print('Collection name already exists')


    def getRecords(self, db_name, collection_name):
        """This fetches collection data from database."""
        try:
            collection = self.client[str(db_name)][str(collection_name)]
            data = collection.find()
            log.info(f"Fetching records from collection")
            return data
        except Exception as e:
            log.info("[getRecords]:Problem occured while fetching data" + str(e))
            print("[getRecords]:Problem occured while fetching data")
    

    def insert(self, db_name, collection_name, record):
        """clean the dataset and insert data into mongodb database"""
        log.info('execute the mongodb insertion function')
        try:
            #insert data into collection
            if type(record) == dict:
                self.client[str(db_name)][str(collection_name)].insert_one(record)
                log.error('Record inserted successfully')
            elif type(record) == list:
                self.client[str(db_name)][str(collection_name)].insert_many(record)
                log.error('Record inserted successfully')
        except Exception as e:
            log.error('Insertion operation error occured', e)
            print('Insertion operation error occured')
        else:
            print('Insertion operation successfully completed')
    
            
    def update(self, db_name, collection_name, old_value, new_value):
        """Update data set with new values"""
        try:
            #update many operation
            self.client[str(db_name)][str(collection_name)].update_many(old_value, {'$set': new_value})
        except Exception as e:
            log.error('Update error', e)
            print('Update error')
        else:
            print('Update successfully completed')
    

    def delete(self, db_name, collection_name, delete_data):
        """Delete operation on the dataset"""
        try:
            #delete many element of a collection based on some conditions
            self.client[str(db_name)][str(collection_name)].delete_many(delete_data)
        except Exception as e:
            log.error('Delete operation error occured', e)
            print('Delete operation error occured')
        else:
            print('Delete operation successfully completed')
    

    def find(self, db_name, collection_name, query):
        """find a particular set of data"""
        return self.client[str(db_name)][str(collection_name)].find(query)

