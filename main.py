from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

# Configuration
PROJECT_ID = "mgmt545-491814"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    try:
        yield client
    finally:
        client.close()

# ---------------------------------------------------------------------------
# Data Models (Pydantic)
# ---------------------------------------------------------------------------

class Property(BaseModel):
    property_id: Optional[int] = None
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    monthly_rent: float
    tenant_name: Optional[str] = None

class Income(BaseModel):
    income_id: Optional[int] = None
    property_id: int
    amount: float
    source: str
    date: str
    description: Optional[str] = None

class Expense(BaseModel):
    expense_id: Optional[int] = None
    property_id: int
    amount: float
    category: str
    date: str
    description: Optional[str] = None

# ---------------------------------------------------------------------------
# Endpoints: Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """Returns all properties in the database."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` ORDER BY property_id"
    try:
        results = bq.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns a single property by its ID."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    try:
        results = list(bq.query(query).result())
        if not results:
            raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
        return dict(results[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Endpoints: Income
# ---------------------------------------------------------------------------

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns all income records for a specific property."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}"
    try:
        results = bq.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/income/{property_id}", status_code=status.HTTP_201_CREATED)
def create_income(property_id: int, income: Income, bq: bigquery.Client = Depends(get_bq_client)):
    """Adds a new income record for a property."""
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income` (property_id, amount, source, date, description)
        VALUES ({property_id}, {income.amount}, '{income.source}', '{income.date}', '{income.description or ""}')
    """
    try:
        bq.query(query).result()
        return {"message": "Income record created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Endpoints: Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Returns all expense records for a specific property."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}"
    try:
        results = bq.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/expenses/{property_id}", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, expense: Expense, bq: bigquery.Client = Depends(get_bq_client)):
    """Adds a new expense record for a property."""
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses` (property_id, amount, category, date, description)
        VALUES ({property_id}, {expense.amount}, '{expense.category}', '{expense.date}', '{expense.description or ""}')
    """
    try:
        bq.query(query).result()
        return {"message": "Expense record created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Endpoints: Analytics
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/performance")
def get_property_performance(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Calculates total income, total expenses, and net profit for a property.
    """
    # Query for total income
    income_query = f"SELECT SUM(amount) as total FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}"
    # Query for total expenses
    expense_query = f"SELECT SUM(amount) as total FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}"
    
    try:
        income_result = list(bq.query(income_query).result())
        expense_result = list(bq.query(expense_query).result())
        
        total_income = income_result[0].total or 0.0
        total_expenses = expense_result[0].total or 0.0
        net_profit = total_income - total_expenses
        
        return {
            "property_id": property_id,
            "total_income": total_income,
            "total_expenses": total_expenses,
            "net_profit": net_profit
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))