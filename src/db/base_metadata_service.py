import datetime


class BaseMetadataService:

    def initialize_metadata_source(self):
        raise NotImplementedError

    def get_engine(self):
        raise NotImplementedError

    def insert_into_db(self, df_to_insert, chunksize, table_name, method):
        raise NotImplementedError
