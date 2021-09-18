import urllib.parse
from sqlalchemy import create_engine

urllib.parse.quote_plus("kx%jj5/g")

engine = create_engine('postgresql://scott:tiger@localhost:5432/mydatabase')
engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')

%load_ext sql