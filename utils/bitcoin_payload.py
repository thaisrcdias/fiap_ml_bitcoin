from pydantic import BaseModel

class BitcoinPayload(BaseModel):
    Date: str
    Open: float
    High: float
    Low: float
    Close: float
    Volume: int