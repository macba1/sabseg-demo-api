"""
Sabseg Demo — Backend API
==========================
FastAPI server for the reconciliation engine.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import os

from reconciliation import run_reconciliation, run_triangular_reconciliation

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


@app.post("/api/reconcile-triangular")
async def reconcile_triangular(
    file_a: UploadFile = File(..., description="Fichero A — Estadística de venta (Excel)"),
    file_b: UploadFile = File(..., description="Fichero B — Contabilidad (Excel)"),
    file_c: UploadFile = File(..., description="Fichero C — Liquidaciones de aseguradoras (Excel)"),
):
    """
    Upload three Excel files and run triangular reconciliation (A vs B + A vs C).
    Returns JSON with summary + discrepancies, including those from insurer liquidations.
    """
    for f, label in [(file_a, "Fichero A"), (file_b, "Fichero B"), (file_c, "Fichero C")]:
        if not f.filename.endswith((".xlsx", ".xls", ".csv")):
            raise HTTPException(
                status_code=400,
                detail=f"{label} debe ser un fichero Excel (.xlsx/.xls) o CSV (.csv)",
            )

    try:
        bytes_a = await file_a.read()
        bytes_b = await file_b.read()
        bytes_c = await file_c.read()

        result = run_triangular_reconciliation(bytes_a, bytes_b, bytes_c)

        if "error" in result:
            raise HTTPException(status_code=422, detail=result["error"])

        return JSONResponse(content=result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando ficheros: {str(e)}")


@app.post("/api/demo")
async def demo(triangular: bool = Query(False, description="Si true, carga los 3 ficheros de demo incluyendo liquidaciones")):
    """
    Run reconciliation with pre-loaded demo data.
    No file upload needed — uses bundled sample files.
    Use ?triangular=true to include insurer liquidation file.
    """
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    file_a_path = os.path.join(data_dir, "A_Estadistica_Venta.xlsx")
    file_b_path = os.path.join(data_dir, "B_Contabilidad.xlsx")
    file_c_path = os.path.join(data_dir, "C_Liquidaciones_Aseguradoras.xlsx")

    if not os.path.exists(file_a_path) or not os.path.exists(file_b_path):
        raise HTTPException(status_code=404, detail="Demo data files not found.")

    if triangular and not os.path.exists(file_c_path):
        raise HTTPException(status_code=404, detail="Demo liquidations file (C) not found.")

    try:
        with open(file_a_path, "rb") as f:
            bytes_a = f.read()
        with open(file_b_path, "rb") as f:
            bytes_b = f.read()

        if triangular:
            with open(file_c_path, "rb") as f:
                bytes_c = f.read()
            result = run_triangular_reconciliation(bytes_a, bytes_b, bytes_c)
        else:
            result = run_reconciliation(bytes_a, bytes_b)

        return JSONResponse(content=result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
