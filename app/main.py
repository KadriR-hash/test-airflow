from fastapi import FastAPI

from routers import connections, dags, tasks

app = FastAPI()

app.include_router(connections.router)
app.include_router(dags.router)


# app.include_router(tasks.router)


@app.get("/")
def read_root():
    return {"owner": "@Raed"}


@app.get("/health")
def health_check():
    return {"status": "ok"}
