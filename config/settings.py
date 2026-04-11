from pydantic import BaseModel

class DataSettings(BaseModel):
    start_year: int = 2024
    end_year: int = 2026