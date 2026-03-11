from pydantic import BaseModel
from typing import Optional
from datetime import datetime   

class Auction(BaseModel):
    auction_title: str
    auction_date: datetime
    auction_city: str
    auction_starting_price: float
    auction_target_price: float
    auction_url: str
    car_make: str
    car_model: str
    vin_number: Optional[str] = None
    plate_number: Optional[str] = None
    car_made_year: Optional[int] = None
    otomoto_auctions_count: Optional[int] = None
    otomoto_avg_price: Optional[float] = None
    otomoto_avg_mileage: Optional[int] = None

"""
a.auction_title,
                a.auction_date,
                a.auction_title ,
                a.auction_city ,
                a.starting_price ,
                a.target_price ,
                a.auction_url ,
                cm.make_name ,
                cm2.model_name ,
                a.vin_number ,
                a.plate_number ,
                a.made_year,
                aos.total_otomoto_auctions_count ,
                aos.avg_price ,
                aos.avg_mileage
"""
        