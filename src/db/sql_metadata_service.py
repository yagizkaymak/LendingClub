import sqlalchemy
from sqlalchemy import create_engine, Column
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import subprocess

from db.base_metadata_service import BaseMetadataService
from configuration import Configuration


Base = sqlalchemy.ext.declarative.declarative_base()

class SQLMetadata(Base):
    __tablename__ = 'loan'
    id = Column(sqlalchemy.types.INT, primary_key=True, nullable=False)
    member_id = Column(sqlalchemy.types.INT, nullable=False)
    loan_amnt = Column(sqlalchemy.types.FLOAT, nullable=False)
    funded_amnt = Column(sqlalchemy.types.FLOAT, nullable=False)
    funded_amnt_inv = Column(sqlalchemy.types.FLOAT, nullable=False)
    term = Column(sqlalchemy.types.INT, nullable=False)
    int_rate = Column(sqlalchemy.types.FLOAT, nullable=False)
    installment = Column(sqlalchemy.types.FLOAT, nullable=False)
    grade = Column(sqlalchemy.types.VARCHAR(1), nullable=False)
    sub_grade = Column(sqlalchemy.types.VARCHAR(2), nullable=False)
    emp_title = Column(sqlalchemy.types.VARCHAR(100), nullable=True)
    emp_length = Column(sqlalchemy.types.INT, nullable=False)
    home_ownership = Column(sqlalchemy.types.VARCHAR(50), nullable=False)
    annual_inc = Column(sqlalchemy.types.FLOAT, nullable=True)
    verification_status = Column(sqlalchemy.types.VARCHAR(20), nullable=False)
    issue_d = Column(sqlalchemy.types.DATE, nullable=False)
    loan_status = Column(sqlalchemy.types.VARCHAR(100), nullable=False)
    pymnt_plan = Column(sqlalchemy.types.VARCHAR(1), nullable=False)
    url = Column(sqlalchemy.types.VARCHAR(100), nullable=False)
    desc = Column(sqlalchemy.types.VARCHAR(400), nullable=True)
    purpose = Column(sqlalchemy.types.VARCHAR(100), nullable=False)
    title = Column(sqlalchemy.types.VARCHAR(100), nullable=True)
    zip_code = Column(sqlalchemy.types.VARCHAR(5), nullable=False)
    addr_state = Column(sqlalchemy.types.VARCHAR(2), nullable=False)
    dti = Column(sqlalchemy.types.FLOAT, nullable=False)
    delinq_2yrs = Column(sqlalchemy.types.INT, nullable=False)
    earliest_cr_line = Column(sqlalchemy.types.DATE, nullable=True)
    inq_last_6mths = Column(sqlalchemy.types.INT, nullable=False)
    mths_since_last_delinq = Column(sqlalchemy.types.INT, nullable=True)
    mths_since_last_record = Column(sqlalchemy.types.INT, nullable=True)
    open_acc = Column(sqlalchemy.types.INT, nullable=True)
    pub_rec = Column(sqlalchemy.types.INT, nullable=True)
    revol_bal = Column(sqlalchemy.types.FLOAT, nullable=False)
    revol_util = Column(sqlalchemy.types.FLOAT, nullable=False)
    total_acc = Column(sqlalchemy.types.INT, nullable=False)
    initial_list_status = Column(sqlalchemy.types.VARCHAR(1), nullable=False)
    out_prncp = Column(sqlalchemy.types.FLOAT, nullable=False)
    out_prncp_inv = Column(sqlalchemy.types.FLOAT, nullable=False)
    total_pymnt = Column(sqlalchemy.types.FLOAT, nullable=False)
    total_pymnt_inv = Column(sqlalchemy.types.FLOAT, nullable=False)
    total_rec_prncp = Column(sqlalchemy.types.FLOAT, nullable=False)
    total_rec_int = Column(sqlalchemy.types.FLOAT, nullable=False)
    total_rec_late_fee = Column(sqlalchemy.types.FLOAT, nullable=False)
    recoveries = Column(sqlalchemy.types.FLOAT, nullable=False)
    collection_recovery_fee = Column(sqlalchemy.types.FLOAT, nullable=False)
    last_pymnt_d = Column(sqlalchemy.types.DATE, nullable=True)
    last_pymnt_amnt = Column(sqlalchemy.types.FLOAT, nullable=False)
    next_pymnt_d = Column(sqlalchemy.types.DATE, nullable=True)
    last_credit_pull_d = Column(sqlalchemy.types.DATE, nullable=True)
    collections_12_mths_ex_med = Column(sqlalchemy.types.INT, nullable=True)
    mths_since_last_major_derog = Column(sqlalchemy.types.INT, nullable=True)
    policy_code = Column(sqlalchemy.types.INT, nullable=False)
    application_type = Column(sqlalchemy.types.VARCHAR(20), nullable=False)
    annual_inc_joint = Column(sqlalchemy.types.FLOAT, nullable=True)
    dti_joint = Column(sqlalchemy.types.FLOAT, nullable=True)
    verification_status_joint = Column(sqlalchemy.types.VARCHAR(20), nullable=True)
    acc_now_delinq = Column(sqlalchemy.types.INT, nullable=False)
    tot_coll_amt = Column(sqlalchemy.types.FLOAT, nullable=False)
    tot_cur_bal = Column(sqlalchemy.types.FLOAT, nullable=False)
    open_acc_6m = Column(sqlalchemy.types.INT, nullable=True)
    open_il_6m = Column(sqlalchemy.types.INT, nullable=True)
    open_il_12m = Column(sqlalchemy.types.INT, nullable=True)
    open_il_24m = Column(sqlalchemy.types.INT, nullable=True)
    mths_since_rcnt_il = Column(sqlalchemy.types.INT, nullable=True)
    total_bal_il = Column(sqlalchemy.types.FLOAT, nullable=True)
    il_util = Column(sqlalchemy.types.FLOAT, nullable=True)
    open_rv_12m = Column(sqlalchemy.types.INT, nullable=True)
    open_rv_24m = Column(sqlalchemy.types.INT, nullable=True)
    max_bal_bc = Column(sqlalchemy.types.FLOAT, nullable=True)
    all_util = Column(sqlalchemy.types.FLOAT, nullable=True)
    total_rev_hi_lim = Column(sqlalchemy.types.FLOAT, nullable=True)
    inq_fi = Column(sqlalchemy.types.INT, nullable=True)
    total_cu_tl = Column(sqlalchemy.types.INT, nullable=True)
    inq_last_12m = Column(sqlalchemy.types.INT, nullable=True)


class SQLMetadataService(BaseMetadataService):


    def __init__(self, sql_alchemy_conn, logger):
        logger.debug("Creating MetadataServer (type:SQLMetadataService) with Args - sql_alchemy_conn: {sql_alchemy_conn}, logger: {logger}".format(**locals()))
        self.sql_alchemy_conn = sql_alchemy_conn
        self.logger = logger
        engine_args = {}
        self.engine = create_engine(sql_alchemy_conn, **engine_args)

    def initialize_metadata_source(self):

        # Creating metadata table... Check if already exists.
        if not self.engine.dialect.has_table(self.engine, 'loan'):
            try:
                self.logger.info("Creating Metadata Table")
                Base.metadata.create_all(self.engine)
            except (Exception) as e:
                self.logger.info("Exception while Creating Metadata Table: " + str(e))
                self.logger.info("Table might already exist. Suppressing Exception.")

    def get_engine(self):
        return self.engine


    def insert_into_db(self, df_to_insert, chunksize, table_name, method):
        try:
            self.logger.info("Loan DF is being inserted to DB...Chunksize = " + str(chunksize))

            df_to_insert.to_sql(name=table_name, con=self.engine, if_exists='replace', chunksize=chunksize, index=False, method='multi')
            self.logger.info("Loan DF has been successfully inserted to DB")
        except (Exception) as e:
            self.logger.error("Error during DB insertion from DF " + str(e))
