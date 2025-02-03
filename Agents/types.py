from enumEnum import Enum
from pydantic import BaseModel

class AgentType(str, Enum):
    MAIN_FEED = "main"
