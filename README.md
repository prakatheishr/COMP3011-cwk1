# COMP3011-cwk1

## Dataset Setup

1. Download the dataset from Kaggle:
   https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020

2. Extract the CSV files into:
   data/

3. Run:
   python <>

## Running
uvicorn app.f1api:app --reload

## Example requests
GET /drivers?limit=10
GET /races?year=2024
GET /races/{raceId}
GET /races/{raceId}/results
GET /seasons/{year}/driver-standings
GET /drivers/{driverId}/seasons/{year}?include_results=true
