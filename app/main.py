from fastapi import FastAPI

app = FastAPI()
api = FastAPI()

app.mount("/api", api)
