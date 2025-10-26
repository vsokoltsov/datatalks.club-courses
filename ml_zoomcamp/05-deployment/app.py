from fastapi import FastAPI
from pydantic import BaseModel, PositiveFloat
import pickle

api = FastAPI(
    title="HW5 API",
    description="Simple API for HW5",
    version="0.1.0",
)

class PredictResponse(BaseModel):
    proba: PositiveFloat

class PredictRequest(BaseModel):
    lead_source: str
    number_of_courses_viewed: int
    annual_income: float 

@api.post("/v1/predict", response_model=PredictResponse)
async def predict(
    req: PredictRequest
) -> PredictResponse:
    with open("/code/pipeline_v2.bin", "rb") as f:
        pipeline = pickle.load(f)
    params = dict(req)
    proba = pipeline.predict_proba(params)
    
    return PredictResponse(proba=proba[0][1])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:api", host="0.0.0.0", port=8000, reload=True, log_level="info")