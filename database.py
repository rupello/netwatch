import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime

class Database(object):
    def __init__(self,path=':memory:'):
        self.engine = create_engine('sqlite:///%s' % path, echo=False)
        self.metadata = MetaData()
        self.accepts = Table('accepts', self.metadata,
            Column('id',Integer, primary_key=True),
            Column('timestamp', DateTime),
            Column('proto',String),
            Column('src',String),
            Column('dst',String),
            Column('spt',Integer),
            Column('dpt',Integer),
            Column('inp',String),
            Column('out',String)
            )
        # create the tables, if necessary
        self.metadata.create_all(self.engine) 
        #self.conn = engine.connect()

    def execute(self,ins):
        return self.engine.execute(ins)

