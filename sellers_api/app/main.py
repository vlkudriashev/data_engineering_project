# Part of the code - courtesy of Josh Di Mella
# https://joshdimella.com/blog/adding-api-key-auth-to-fast-api

from fastapi import Security, FastAPI
from fastapi.security import APIKeyHeader
import datetime
import random



api_key = "9d207bf0-10f5-4d8f-a479-22ff5aeff8d1"
api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)
app = FastAPI()

def get_api_key(
    api_key_header: str = Security(api_key_header)
) -> str:
    """Retrieve and validate an API key from the query parameters or HTTP header.
    Args:
        api_key_header: The API key passed in the HTTP header.
    Returns:
        The validated API key.
    Raises:
        HTTPException: If the API key is invalid or missing.
    """
    if api_key_header in API_KEYS:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key",
    )

@app.get("/sellers")
def private(api_key: str = Security(api_key_header)):

    total_records = random.randint(30,50)

    seller_iter = (
        {'seller_id': num,
         'status': random.choice(['basic', 'premium']),
         'risk_score': round(random.uniform(0, 1), 4)
        } for num in range(total_records)
    )

    return list(seller_iter)
