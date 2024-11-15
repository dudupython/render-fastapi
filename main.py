from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import pandas as pd
from get_data import stock_wide_format, stock_long_format
from fastapi.middleware.cors import CORSMiddleware


# Generate the mock data on app startup
# df = pd.read_csv("https://raw.githubusercontent.com/lf2foce/test_data/refs/heads/main/stock_data.csv")
app = FastAPI()
# app = FastAPI(docs_url="/api/py/docs", openapi_url="/api/py/openapi.json")
df = stock_long_format(symbols=['CTG', 'VCB', 'HPG', 'CTD', 'VIC', 'SAB', 'MWG','FPT','FRT'])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://choncophieu.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class StockData(BaseModel):
    date: str
    price: float

@app.get("/query", response_model=List[StockData])
async def get_stock_data(symbol: str):
    # Filter the DataFrame for the requested symbol
    symbol_data = df[df["symbol"] == symbol]
    
    # Check if data exists for the symbol
    if symbol_data.empty:
        raise HTTPException(status_code=404, detail="Symbol not found")

    # Format the data in the required structure and return as list of dictionaries
    recent_data = symbol_data[["date", "close"]].rename(columns={"close": "price"}).to_dict(orient="records")
    return recent_data

@app.get("/helloFastApi")
def hello_fast_api():
    return {"message": "Hello from FastAPI"}


@app.get("/")
async def root():
    return {"message": "Hello World 123"}
