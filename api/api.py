from fastapi import Depends, FastAPI, HTTPException
from db_connectors.api_crud import get_all_auctions_details
from api.api_models import Auction


app = FastAPI()

@app.get("/auctions", response_model=list[Auction])
async def list_auctions(
    make: str | None = None,
    model: str | None = None,
    year: int | None = None,
    city: str | None = None,
    date: str | None = None,
):
    results = get_all_auctions_details(make, model,year, city, date)
    return [dict(zip(Auction.model_fields.keys(), row)) for row in results]
