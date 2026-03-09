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
from normalization import run_normalization

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


# ─── NORMALIZATION ENDPOINTS ──────────────────────────────────────────────────

@app.post("/api/normalize")
async def normalize(
    file: UploadFile = File(..., description="Fichero de correduría a normalizar (Excel)"),
):
    """
    Upload a brokerage Excel file and normalize to canonical model.
    """
    if not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(status_code=400, detail="El fichero debe ser Excel (.xlsx/.xls) o CSV (.csv)")

    try:
        file_bytes = await file.read()
        result = run_normalization(file_bytes, file.filename)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando fichero: {str(e)}")


@app.post("/api/normalize-batch")
async def normalize_batch(
    files: list[UploadFile] = File(..., description="Ficheros de corredurías a normalizar"),
):
    """
    Upload multiple brokerage files and normalize all to canonical model.
    """
    results = []
    for f in files:
        try:
            file_bytes = await f.read()
            result = run_normalization(file_bytes, f.filename)
            results.append(result)
        except Exception as e:
            results.append({"filename": f.filename, "error": str(e)})

    # Summary across all files
    total_records = sum(r.get("total_records", 0) for r in results if "error" not in r)
    total_issues = sum(r.get("total_quality_issues", 0) for r in results if "error" not in r)
    countries = [r.get("detected_country", "??") for r in results if "error" not in r]

    return JSONResponse(content={
        "total_files": len(results),
        "total_records": total_records,
        "total_quality_issues": total_issues,
        "countries_detected": countries,
        "results": results,
    })


@app.post("/api/normalize-demo")
async def normalize_demo():
    """
    Run normalization with pre-loaded demo data (3 brokerages: ES/PT/IT).
    """
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    demo_files = [
        ("Pack_A_Correduria_Espana.xlsx", "Correduría España"),
        ("Pack_B_Correduria_Portugal.xlsx", "Correduría Portugal"),
        ("Pack_C_Correduria_Italia.xlsx", "Correduría Italia"),
    ]

    results = []
    for filename, label in demo_files:
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            results.append({"filename": filename, "error": f"File not found: {filename}"})
            continue
        try:
            with open(filepath, "rb") as f:
                file_bytes = f.read()
            result = run_normalization(file_bytes, filename)
            result["label"] = label
            results.append(result)
        except Exception as e:
            results.append({"filename": filename, "error": str(e)})

    total_records = sum(r.get("total_records", 0) for r in results if "error" not in r)
    total_issues = sum(r.get("total_quality_issues", 0) for r in results if "error" not in r)
    countries = [r.get("detected_country", "??") for r in results if "error" not in r]

    return JSONResponse(content={
        "total_files": len(results),
        "total_records": total_records,
        "total_quality_issues": total_issues,
        "countries_detected": countries,
        "results": results,
    })
