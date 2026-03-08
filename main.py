"""
Sabseg Demo — Backend API
==========================
FastAPI server for the reconciliation engine.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os

from reconciliation import run_reconciliation

app = FastAPI(title="Sabseg Reconciliation Demo", version="1.0.0")

# CORS — allow frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your Vercel domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "ok", "service": "Sabseg Reconciliation API", "version": "1.0.0"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/api/reconcile")
async def reconcile(
    file_a: UploadFile = File(..., description="Fichero A — Estadística de venta (Excel)"),
    file_b: UploadFile = File(..., description="Fichero B — Contabilidad (Excel)"),
):
    """
    Upload two Excel files and run reconciliation.
    Returns JSON with summary + discrepancies.
    """
    # Validate file types
    for f, label in [(file_a, "Fichero A"), (file_b, "Fichero B")]:
        if not f.filename.endswith((".xlsx", ".xls", ".csv")):
            raise HTTPException(
                status_code=400,
                detail=f"{label} debe ser un fichero Excel (.xlsx/.xls) o CSV (.csv)",
            )

    try:
        bytes_a = await file_a.read()
        bytes_b = await file_b.read()

        result = run_reconciliation(bytes_a, bytes_b)

        if "error" in result:
            raise HTTPException(status_code=422, detail=result["error"])

        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando ficheros: {str(e)}")


@app.post("/api/demo")
async def demo():
    """
    Run reconciliation with pre-loaded demo data.
    No file upload needed — uses bundled sample files.
    """
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    file_a_path = os.path.join(data_dir, "A_Estadistica_Venta.xlsx")
    file_b_path = os.path.join(data_dir, "B_Contabilidad.xlsx")

    if not os.path.exists(file_a_path) or not os.path.exists(file_b_path):
        raise HTTPException(status_code=404, detail="Demo data files not found.")

    try:
        with open(file_a_path, "rb") as f:
            bytes_a = f.read()
        with open(file_b_path, "rb") as f:
            bytes_b = f.read()

        result = run_reconciliation(bytes_a, bytes_b)
        return JSONResponse(content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
