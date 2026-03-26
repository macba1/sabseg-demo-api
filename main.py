"""
Sabseg Demo — Backend API v2
==============================
FastAPI server with real Sabseg reconciliation.
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List
import os

from reconciliation import run_reconciliation
from normalization import run_normalization
from reconciliation_sabseg import run_sabseg_reconciliation

app = FastAPI(title="Sabseg Demo API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"status": "ok", "service": "Sabseg Demo API", "version": "2.0.0"}


@app.get("/health")
def health():
    return {"status": "healthy"}


# ─── GENERIC RECONCILIATION (synthetic data) ─────────────────────────────────

@app.post("/api/reconcile")
async def reconcile(
    file_a: UploadFile = File(...),
    file_b: UploadFile = File(...),
):
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
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/demo")
async def demo():
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    file_a_path = os.path.join(data_dir, "A_Estadistica_Venta.xlsx")
    file_b_path = os.path.join(data_dir, "B_Contabilidad.xlsx")
    if not os.path.exists(file_a_path) or not os.path.exists(file_b_path):
        raise HTTPException(status_code=404, detail="Demo data not found.")
    try:
        with open(file_a_path, "rb") as f:
            bytes_a = f.read()
        with open(file_b_path, "rb") as f:
            bytes_b = f.read()
        result = run_reconciliation(bytes_a, bytes_b)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── REAL SABSEG RECONCILIATION ──────────────────────────────────────────────

@app.post("/api/reconcile-sabseg")
async def reconcile_sabseg(
    saldos: UploadFile = File(..., description="Saldos Contables (Business Central)"),
    estadisticas: List[UploadFile] = File(..., description="Ficheros de estadísticas de venta"),
):
    """
    Real Sabseg reconciliation: saldos contables vs estadísticas de venta.
    Upload the saldos file + all statistics files.
    """
    try:
        saldos_bytes = await saldos.read()
        stats_files = []
        for f in estadisticas:
            content = await f.read()
            stats_files.append((f.filename, content))
        
        result = run_sabseg_reconciliation(saldos_bytes, stats_files)
        
        if "error" in result:
            raise HTTPException(status_code=422, detail=result["error"])
        
        return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.post("/api/demo-sabseg")
async def demo_sabseg():
    """
    Run real Sabseg reconciliation with pre-loaded data.
    """
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    # Saldos contables
    saldos_path = os.path.join(data_dir, "Saldos_Contables_Ene_y_Feb_2026.xlsx")
    if not os.path.exists(saldos_path):
        raise HTTPException(status_code=404, detail="Saldos contables file not found.")
    
    with open(saldos_path, "rb") as f:
        saldos_bytes = f.read()
    
    # All statistics files
    stats_files = []
    stats_filenames = [
        "2026_02_ELEVIA.xlsx",
        "2026_02 AGRINALCAZAR AGRO.xlsx",
        "2026_02 Araytor.xlsx",
        "2026_02 FUTURA.xlsx",
        "2026_02 INSURART.xlsx",
        "2026_02 SANCHEZ VALENCIA.xlsx",
        "2026_02 SEGURETXE - v2.xlsx",
        "2026_02 Verobroker.xlsx",
        "2026_02 ZURRIOLA.xlsx",
        "02_2026 ADSA AGRO.xlsx",
        "02_2026 AISA AGRO.xlsx",
        "02_2026 BANA AGRO.xlsx",
        "MAURA.pdf",
        "Recibos_ARRENTA_202602.xlsx",
    ]
    
    for fn in stats_filenames:
        fp = os.path.join(data_dir, fn)
        if os.path.exists(fp):
            with open(fp, "rb") as f:
                stats_files.append((fn, f.read()))
    
    try:
        result = run_sabseg_reconciliation(saldos_bytes, stats_files)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ─── NORMALIZATION ────────────────────────────────────────────────────────────

@app.post("/api/normalize")
async def normalize(file: UploadFile = File(...)):
    try:
        file_bytes = await file.read()
        result = run_normalization(file_bytes, file.filename)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/normalize-batch")
async def normalize_batch(files: List[UploadFile] = File(...)):
    results = []
    for f in files:
        try:
            file_bytes = await f.read()
            result = run_normalization(file_bytes, f.filename)
            results.append(result)
        except Exception as e:
            results.append({"filename": f.filename, "error": str(e)})
    
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
            results.append({"filename": filename, "error": f"Not found"})
            continue
        try:
            with open(filepath, "rb") as f:
                result = run_normalization(f.read(), filename)
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
