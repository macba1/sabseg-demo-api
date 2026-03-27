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
from data_quality import run_data_quality
from corrections import apply_corrections, generate_broker_report, generate_all_reports

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


# ─── DATA QUALITY (Case 1) ───────────────────────────────────────────────────

@app.post("/api/data-quality")
async def data_quality(
    files: List[UploadFile] = File(..., description="Ficheros de recibos a validar"),
):
    """Validate brokerage receipt files for data quality issues."""
    try:
        file_list = []
        for f in files:
            content = await f.read()
            file_list.append((f.filename, content))
        
        result = run_data_quality(file_list)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.post("/api/data-quality-demo")
async def data_quality_demo():
    """Run data quality validation with pre-loaded pilot files."""
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    pilot_files = [
        "PILOT_202602_Araytor.xlsx",
        "PILOT_202602_Zurriola.xlsx",
        "PILOT_2026_02_SEGURETXE.xlsx",
        "PILOT_2026_01_ARRENTA.xlsx",
        "PILOT_202602_ARRENTA.xlsx",
    ]
    
    file_list = []
    for fn in pilot_files:
        fp = os.path.join(data_dir, fn)
        if os.path.exists(fp):
            with open(fp, "rb") as f:
                file_list.append((fn, f.read()))
    
    if not file_list:
        raise HTTPException(status_code=404, detail="Pilot files not found.")
    
    try:
        result = run_data_quality(file_list)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ─── CORRECTIONS + REPORTS ────────────────────────────────────────────────────

@app.post("/api/apply-corrections")
async def api_apply_corrections(
    files: List[UploadFile] = File(...),
):
    """Apply auto-corrections to uploaded files. Returns corrected files + report."""
    results = []
    for f in files:
        content = await f.read()
        result = apply_corrections(content, f.filename)
        if 'corrected_file' in result:
            # Don't send raw bytes in JSON, just the stats
            result.pop('corrected_file')
        results.append(result)
    
    total_corrections = sum(r.get('total_corrections', 0) for r in results if 'error' not in r)
    total_remaining = sum(r.get('total_remaining', 0) for r in results if 'error' not in r)
    
    return JSONResponse(content={
        'total_files': len(results),
        'total_corrections': total_corrections,
        'total_remaining': total_remaining,
        'results': results,
    })


@app.post("/api/apply-corrections-demo")
async def api_apply_corrections_demo():
    """Apply corrections to pilot files and return results."""
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    pilot_files = [
        "PILOT_202602_Araytor.xlsx",
        "PILOT_202602_Zurriola.xlsx",
        "PILOT_2026_02_SEGURETXE.xlsx",
        "PILOT_2026_01_ARRENTA.xlsx",
        "PILOT_202602_ARRENTA.xlsx",
    ]
    
    results = []
    for fn in pilot_files:
        fp = os.path.join(data_dir, fn)
        if os.path.exists(fp):
            with open(fp, "rb") as f:
                result = apply_corrections(f.read(), fn)
            if 'corrected_file' in result:
                result.pop('corrected_file')
            results.append(result)
    
    total_corrections = sum(r.get('total_corrections', 0) for r in results if 'error' not in r)
    total_remaining = sum(r.get('total_remaining', 0) for r in results if 'error' not in r)
    
    return JSONResponse(content={
        'total_files': len(results),
        'total_corrections': total_corrections,
        'total_remaining': total_remaining,
        'results': results,
    })


@app.post("/api/generate-reports")
async def api_generate_reports(
    files: List[UploadFile] = File(...),
):
    """Generate broker reports/emails for issues that need manual review."""
    from data_quality import validate_file
    
    validation_results = []
    for f in files:
        content = await f.read()
        result = validate_file(content, f.filename)
        validation_results.append(result)
    
    reports = generate_all_reports(validation_results)
    return JSONResponse(content={'reports': reports})


@app.post("/api/generate-reports-demo")
async def api_generate_reports_demo():
    """Generate broker reports for pilot files."""
    from data_quality import validate_file
    
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    pilot_files = [
        "PILOT_202602_Araytor.xlsx",
        "PILOT_202602_Zurriola.xlsx",
        "PILOT_2026_02_SEGURETXE.xlsx",
        "PILOT_2026_01_ARRENTA.xlsx",
        "PILOT_202602_ARRENTA.xlsx",
    ]
    
    validation_results = []
    for fn in pilot_files:
        fp = os.path.join(data_dir, fn)
        if os.path.exists(fp):
            with open(fp, "rb") as f:
                result = validate_file(f.read(), fn)
            validation_results.append(result)
    
    reports = generate_all_reports(validation_results)
    return JSONResponse(content={'reports': reports})


# ─── QA AGENTS ────────────────────────────────────────────────────────────────

@app.post("/api/qa-reconciliation")
async def qa_reconciliation():
    """Run QA agents on reconciliation results (uses demo data)."""
    from qa_agents import QAOrchestrator
    
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    saldos_path = os.path.join(data_dir, "Saldos_Contables_Ene_y_Feb_2026.xlsx")
    
    with open(saldos_path, "rb") as f:
        saldos_bytes = f.read()
    
    stats_files = []
    for fn in os.listdir(data_dir):
        if fn == "Saldos_Contables_Ene_y_Feb_2026.xlsx" or fn.startswith("PILOT") or fn.startswith("Pack") or fn.startswith("Modelo") or fn == "Piloto_IA_Errores.xlsx":
            continue
        fp = os.path.join(data_dir, fn)
        if os.path.isfile(fp):
            with open(fp, "rb") as f:
                stats_files.append((fn, f.read()))
    
    from reconciliation_sabseg import run_sabseg_reconciliation
    recon_result = run_sabseg_reconciliation(saldos_bytes, stats_files)
    
    qa = QAOrchestrator()
    qa_report = qa.run_reconciliation_qa(recon_result)
    
    return JSONResponse(content=qa_report)


@app.post("/api/qa-data-quality")
async def qa_data_quality():
    """Run QA agents on data quality results (uses demo data)."""
    from qa_agents import QAOrchestrator
    from data_quality import run_data_quality
    
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    pilot_files = [
        "PILOT_202602_Araytor.xlsx",
        "PILOT_202602_Zurriola.xlsx",
        "PILOT_2026_02_SEGURETXE.xlsx",
        "PILOT_2026_01_ARRENTA.xlsx",
        "PILOT_202602_ARRENTA.xlsx",
    ]
    
    file_list = []
    for fn in pilot_files:
        fp = os.path.join(data_dir, fn)
        if os.path.exists(fp):
            with open(fp, "rb") as f:
                file_list.append((fn, f.read()))
    
    dq_result = run_data_quality(file_list)
    
    qa = QAOrchestrator()
    qa_report = qa.run_data_quality_qa(dq_result)
    
    return JSONResponse(content=qa_report)


# ─── LOGGED PROCESSING (with agent activity panel) ────────────────────────────

@app.post("/api/reconcile-logged")
async def reconcile_logged(
    saldos: UploadFile = File(...),
    estadisticas: List[UploadFile] = File(...),
):
    """Reconciliation with agent activity log."""
    from logged_processing import run_reconciliation_logged
    try:
        saldos_bytes = await saldos.read()
        stats_files = []
        for f in estadisticas:
            content = await f.read()
            stats_files.append((f.filename, content))
        result = run_reconciliation_logged(saldos_bytes, stats_files)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/demo-sabseg-logged")
async def demo_sabseg_logged():
    """Demo reconciliation with agent activity log."""
    from logged_processing import run_reconciliation_logged
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    saldos_path = os.path.join(data_dir, "Saldos_Contables_Ene_y_Feb_2026.xlsx")
    with open(saldos_path, "rb") as f:
        saldos_bytes = f.read()
    
    stats_files = []
    for fn in os.listdir(data_dir):
        if fn == "Saldos_Contables_Ene_y_Feb_2026.xlsx" or fn.startswith("PILOT") or fn.startswith("Pack") or fn.startswith("Modelo") or fn == "Piloto_IA_Errores.xlsx":
            continue
        fp = os.path.join(data_dir, fn)
        if os.path.isfile(fp):
            with open(fp, "rb") as f:
                stats_files.append((fn, f.read()))
    
    try:
        result = run_reconciliation_logged(saldos_bytes, stats_files)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-quality-logged")
async def data_quality_logged(
    files: List[UploadFile] = File(...),
):
    """Data quality validation with agent activity log."""
    from logged_processing import run_data_quality_logged
    try:
        file_list = []
        for f in files:
            content = await f.read()
            file_list.append((f.filename, content))
        result = run_data_quality_logged(file_list)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-quality-demo-logged")
async def data_quality_demo_logged():
    """Demo data quality with agent activity log."""
    from logged_processing import run_data_quality_logged
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    pilot_files = [
        "PILOT_202602_Araytor.xlsx",
        "PILOT_202602_Zurriola.xlsx",
        "PILOT_2026_02_SEGURETXE.xlsx",
        "PILOT_2026_01_ARRENTA.xlsx",
        "PILOT_202602_ARRENTA.xlsx",
    ]
    
    file_list = []
    for fn in pilot_files:
        fp = os.path.join(data_dir, fn)
        if os.path.exists(fp):
            with open(fp, "rb") as f:
                file_list.append((fn, f.read()))
    
    try:
        result = run_data_quality_logged(file_list)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
