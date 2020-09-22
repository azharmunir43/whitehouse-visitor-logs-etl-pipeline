
import os
from pathlib import Path

# path to root of lake's mounted directory
DATA_LAKE = Path(os.environ.get("DATA_LAKE",
                                "data_lake/"))

# name of project's sub directory inside lake
WHITEHOUSE_LOGS = 'whitehouse_visitor_logs'
