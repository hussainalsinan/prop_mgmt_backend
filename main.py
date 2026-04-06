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

class PropertyUpdate(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    property_type: Optional[str] = None
    monthly_rent: Optional[float] = None
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

@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(prop: Property, bq: bigquery.Client = Depends(get_bq_client)):
    """Adds a new property to the system."""
    max_id_query = f"SELECT IFNULL(MAX(property_id), 0) + 1 AS new_id FROM `{PROJECT_ID}.{DATASET}.properties`"
    new_id = list(bq.query(max_id_query).result())[0]["new_id"]
    tenant = f"'{prop.tenant_name}'" if prop.tenant_name else "NULL"
    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
            (property_id, name, address, city, state, postal_code, property_type, monthly_rent, tenant_name)
        VALUES
            ({new_id}, '{prop.name}', '{prop.address}', '{prop.city}', '{prop.state}',
             '{prop.postal_code}', '{prop.property_type}', {prop.monthly_rent}, {tenant})
    """
    try:
        bq.query(query).result()
        return {**prop.dict(), "property_id": new_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/properties/{property_id}")
def update_property(property_id: int, prop: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    """Updates one or more fields on an existing property."""
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id} LIMIT 1").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    updates = []
    if prop.name is not None:          updates.append(f"name = '{prop.name}'")
    if prop.address is not None:       updates.append(f"address = '{prop.address}'")
    if prop.city is not None:          updates.append(f"city = '{prop.city}'")
    if prop.state is not None:         updates.append(f"state = '{prop.state}'")
    if prop.postal_code is not None:   updates.append(f"postal_code = '{prop.postal_code}'")
    if prop.property_type is not None: updates.append(f"property_type = '{prop.property_type}'")
    if prop.monthly_rent is not None:  updates.append(f"monthly_rent = {prop.monthly_rent}")
    if prop.tenant_name is not None:   updates.append(f"tenant_name = '{prop.tenant_name}'")
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")
    try:
        bq.query(f"UPDATE `{PROJECT_ID}.{DATASET}.properties` SET {', '.join(updates)} WHERE property_id = {property_id}").result()
        return get_property(property_id, bq)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Removes a property from the system."""
    check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id} LIMIT 1").result())
    if not check:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")
    try:
        bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result()
        return {"message": f"Property {property_id} deleted successfully"}
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
    income_query = f"SELECT SUM(amount) as total FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}"
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