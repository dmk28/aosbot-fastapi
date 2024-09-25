from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from typing import List
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import asyncio
import aiomysql

# Assume GetAirTables class is imported or defined elsewhere
from database.airtable_connect import GetAirTables

# Global variables
airtables = None
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    global airtables, db_pool
    
    # Initialize AirTables
    airtables = GetAirTables()  # Initialize with your base_id and access_token
    
    # Create database pool
    db_pool = await aiomysql.create_pool(
        host='127.0.0.1', port=3306,
        user='root', password='', db='mysql',
        minsize=1, maxsize=10, autocommit=True
    )
    
    print("Startup complete")
    yield
    # Shutdown logic
    if db_pool:
        db_pool.close()
        await db_pool.wait_closed()
    print("Shutdown complete")

app = FastAPI(lifespan=lifespan)

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Authentication
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Depends(api_key_header)):
    if api_key_header is None:
        raise HTTPException(status_code=400, detail="X-API-Key header invalid")
    # Here you would validate the API key against your database
    # For this example, we'll use a dummy check
    if api_key_header != "valid_api_key":
        raise HTTPException(status_code=403, detail="Could not validate credentials")
    return api_key_header

class Query(BaseModel):
    client_full_name: str
    client_id: str

@app.get("/protected")
async def protected_route(user_id: int = Depends(get_api_key)):
    return {"message": f"Access granted for user {user_id}"}

@app.get("/table/{table_id}")
@limiter.limit("100/minute")
async def get_table(table_id: str, user_id: int = Depends(get_api_key)):
    try:
        df = await asyncio.to_thread(airtables.get_table, table_id)
        return {"user_id": user_id, "data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/record_id/{table_id}")
@limiter.limit("200/minute")
async def get_record_id(
    table_id: str, 
    id_column: str, 
    name_column: str, 
    query: Query, 
    user_id: int = Depends(get_api_key)
):
    record_id = await asyncio.to_thread(
        airtables.get_record_id, 
        table_id, 
        id_column, 
        name_column, 
        (query.client_full_name, query.client_id)
    )
    if record_id:
        return {"user_id": user_id, "record_id": record_id}
    else:
        raise HTTPException(status_code=404, detail="Record not found")

@app.get("/verify_updates/{table_id}")
@limiter.limit("150/minute")
async def verify_updates(
    table_id: str, 
    name_column: str, 
    id_column: str, 
    update_column: str, 
    user_id: int = Depends(get_api_key)
):
    updates = await asyncio.to_thread(
        airtables.verify_update_tables, 
        table_id, 
        name_column, 
        id_column, 
        update_column
    )
    return {"user_id": user_id, "updates": updates}

@app.post("/verify_and_update/{table_id}")
@limiter.limit("50/minute")
async def verify_and_update(
    table_id: str, 
    name_column: str, 
    id_column: str, 
    update_column: str, 
    query: Query, 
    user_id: int = Depends(get_api_key)
):
    result = await asyncio.to_thread(
        airtables.verify_and_update, 
        table_id, 
        name_column, 
        id_column, 
        update_column, 
        (query.client_full_name, query.client_id)
    )
    if result:
        return {"user_id": user_id, "status": "Updated"}
    else:
        raise HTTPException(status_code=404, detail="Record not found or update failed")

@app.get("/comments/{table_id}")
@limiter.limit("100/minute")
async def get_comments(table_id: str, user_id: int = Depends(get_api_key)):
    try:
        df = await asyncio.to_thread(airtables.get_comments, table_id)
        return {"user_id": user_id, "comments": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/comment/{table_id}/{record_id}")
@limiter.limit("50/minute")
async def create_comment(
    table_id: str, 
    record_id: str, 
    comment: str, 
    user_id: int = Depends(get_api_key)
):
    result = await asyncio.to_thread(airtables.create_comment, table_id, record_id, comment)
    if result:
        return {"user_id": user_id, "status": "Comment created"}
    else:
        raise HTTPException(status_code=500, detail="Failed to create comment")

@app.get("/client_database")
@limiter.limit("20/minute")
async def get_client_database(
    main_table_id: str, 
    checking_table_id: str, 
    id_column: str, 
    name_column: str, 
    user_id: int = Depends(get_api_key)
):
    try:
        df = await asyncio.to_thread(
            airtables.get_client_database, 
            main_table_id, 
            checking_table_id, 
            id_column, 
            name_column
        )
        return {"user_id": user_id, "data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)