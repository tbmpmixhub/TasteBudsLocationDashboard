# This file is used to build the database schema in the Digital Ocean cluster
from dotenv import load_dotenv
load_dotenv()

import utils

utils.init_db()
print("Schema created.")