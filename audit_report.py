"""
Informe de Auditoría — Verificación contra fuente
===================================================
Genera un informe detallado que permite verificar manualmente
que los datos se han extraído correctamente de cada fichero.

Para cada fichero muestra:
- Qué leyó el sistema vs qué hay realmente en el fichero
- Primera y última fila de datos (para verificar visualmente)
- Totales calculados vs totales que se pueden verificar en Excel
- Columnas detectadas vs columnas reales
"""

import pandas as pd
import openpyxl
from io import BytesIO
from datetime import datetime
import os


def generate_audit_report(data_dir):
    """
    Generate a full audit report for all data files.
    Returns a dict with verification data for each file.
    """
    report = {
        'generated': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'purpose': 'Verificación manual contra fuente. Revisa que los datos extraídos coinciden con los ficheros originales.',
        'files': [],
    }
    
    # ── SALDOS CONTABLES ─────────────────────────────────────────────────
    saldos_path = os.path.join(data_dir, 'Saldos_Contables_Ene_y_Feb_2026.xlsx')
    if os.path.exists(saldos_path):
        try:
            wb = openpyxl.load_workbook(saldos_path, read_only=True)
            saldos_info = {
                'filename': 'Saldos_Contables_Ene_y_Feb_2026.xlsx',
                'tipo': 'Saldos contables (Business Central)',
                'pestañas': wb.sheetnames,
                'meses': [],
            }
            
            for month_label in ['Ene-26', 'Feb-26']:
                if month_label in wb.sheetnames:
                    ws = wb[month_label]
                    rows = list(ws.iter_rows(min_row=1, values_only=True))
                    
                    # Header
                    header = [str(h) for h in rows[0] if h] if rows else []
                    
                    # Count
                    data_rows = [r for r in rows[1:] if r[0] is not None]
                    
                    # Empresas
                    empresas = set(str(r[0]).strip() for r in data_rows if r[0])
                    
                    # Sum 705 and 623
                    sum_705 = 0
                    sum_623 = 0
                    cuentas_705 = {}
                    cuentas_623 = {}
                    for r in data_rows:
                        if r[1] and r[3]:
                            num = str(r[1]).strip()
                            saldo = float(r[3]) if r[3] else 0
                            if num.startswith('705'):
                                sum_705 += saldo
                                cuentas_705[num] = cuentas_705.get(num, 0) + saldo
                            elif num.startswith('623'):
                                sum_623 += saldo
                                cuentas_623[num] = cuentas_623.get(num, 0) + saldo
                    
                    # Sample rows
                    first_3 = []
                    for r in data_rows[:3]:
                        first_3.append({
                            'empresa': str(r[0])[:40] if r[0] else '',
                            'cuenta': str(r[1]) if r[1] else '',
                            'nombre': str(r[2])[:30] if r[2] else '',
                            'saldo_periodo': round(float(r[3]), 2) if r[3] else 0,
                        })
                    
                    last_3 = []
                    for r in data_rows[-3:]:
                        last_3.append({
                            'empresa': str(r[0])[:40] if r[0] else '',
                            'cuenta': str(r[1]) if r[1] else '',
                            'nombre': str(r[2])[:30] if r[2] else '',
                            'saldo_periodo': round(float(r[3]), 2) if r[3] else 0,
                        })
                    
                    saldos_info['meses'].append({
                        'mes': month_label,
                        'header': header,
                        'total_filas': len(data_rows),
                        'empresas_encontradas': len(empresas),
                        'lista_empresas': sorted(empresas),
                        'sum_705': round(sum_705, 2),
                        'sum_623': round(sum_623, 2),
                        'cuentas_705': {k: round(v, 2) for k, v in sorted(cuentas_705.items())},
                        'cuentas_623': {k: round(v, 2) for k, v in sorted(cuentas_623.items())},
                        'primeras_3_filas': first_3,
                        'ultimas_3_filas': last_3,
                        'verificacion': f'Abre el Excel, pestaña {month_label}. Verifica que hay {len(data_rows)} filas de datos, {len(empresas)} empresas, y que la suma de 705xxx = {round(sum_705, 2)} y 623xxx = {round(sum_623, 2)}.',
                    })
            
            wb.close()
            report['files'].append(saldos_info)
        except Exception as e:
            report['files'].append({'filename': 'Saldos_Contables', 'error': str(e)})
    
    # ── ESTADÍSTICAS DE VENTA ────────────────────────────────────────────
    stats_files = [
        ('2026_02_ELEVIA.xlsx', 'Data', 0),
        ('2026_02 AGRINALCAZAR AGRO.xlsx', 'Modelo Datos', 0),
        ('2026_02 Araytor.xlsx', 'Plantilla Report Recibos', 2),
        ('2026_02 FUTURA.xlsx', 'DATOS 2026', 0),
        ('2026_02 INSURART.xlsx', 'Modelo Datos', 0),
        ('2026_02 SANCHEZ VALENCIA.xlsx', 'Sheet1', 0),
        ('2026_02 SEGURETXE - v2.xlsx', 'Data', 2),
        ('2026_02 Verobroker.xlsx', 'Modelo Datos', 0),
        ('2026_02 ZURRIOLA.xlsx', 'IT', 0),
        ('02_2026 ADSA AGRO.xlsx', 'Datos', 1),
        ('02_2026 AISA AGRO.xlsx', 'Datos', 1),
        ('02_2026 BANA AGRO.xlsx', 'Datos', 1),
        ('Recibos_ARRENTA_202602.xlsx', 'Plantilla Report Recibos', 0),
    ]
    
    for filename, sheet_name, header_row in stats_files:
        fp = os.path.join(data_dir, filename)
        if not os.path.exists(fp):
            continue
        
        try:
            # Read with openpyxl to get raw data
            wb = openpyxl.load_workbook(fp, read_only=True)
            
            if sheet_name not in wb.sheetnames:
                report['files'].append({
                    'filename': filename,
                    'error': f'Pestaña "{sheet_name}" no encontrada. Pestañas disponibles: {wb.sheetnames}',
                })
                wb.close()
                continue
            
            ws = wb[sheet_name]
            total_rows = ws.max_row or 0
            total_cols = ws.max_column or 0
            
            # Get headers
            header_row_data = list(ws.iter_rows(min_row=header_row+1, max_row=header_row+1, values_only=True))
            headers = [str(h)[:30] for h in header_row_data[0] if h] if header_row_data else []
            
            # Get first 3 data rows
            first_rows = list(ws.iter_rows(min_row=header_row+2, max_row=header_row+4, values_only=True))
            first_preview = []
            for r in first_rows:
                vals = [str(v)[:25] if v is not None else '' for v in r[:8]]
                first_preview.append(vals)
            
            wb.close()
            
            # Read with pandas for totals
            df = pd.read_excel(fp, sheet_name=sheet_name, header=header_row)
            
            # Find numeric columns for totals
            numeric_summary = {}
            for col in df.columns:
                if any(kw in str(col).lower() for kw in ['comis', 'prima', 'honor', 'com,', 'rcom']):
                    vals = pd.to_numeric(df[col], errors='coerce')
                    if vals.notna().sum() > 0:
                        numeric_summary[str(col)[:30]] = {
                            'sum': round(vals.sum(), 2),
                            'count_nonzero': int((vals != 0).sum()),
                            'min': round(vals.min(), 2) if vals.notna().any() else 0,
                            'max': round(vals.max(), 2) if vals.notna().any() else 0,
                        }
            
            file_info = {
                'filename': filename,
                'tipo': 'Estadísticas de venta',
                'pestaña_leida': sheet_name,
                'header_en_fila': header_row + 1,
                'total_filas_excel': total_rows,
                'total_filas_datos': len(df),
                'total_columnas': total_cols,
                'columnas_detectadas': headers[:15],
                'primeras_filas': first_preview,
                'totales_columnas_clave': numeric_summary,
                'verificacion': f'Abre {filename}, pestaña "{sheet_name}". Headers en fila {header_row+1}. Verifica {len(df)} filas de datos y que los totales de las columnas numéricas coinciden.',
            }
            
            report['files'].append(file_info)
            
        except Exception as e:
            report['files'].append({'filename': filename, 'error': str(e)})
    
    # ── PILOT DATA QUALITY FILES ─────────────────────────────────────────
    pilot_files = [
        ('PILOT_202602_Araytor.xlsx', 'Plantilla Report Recibos', 2),
        ('PILOT_202602_Zurriola.xlsx', 'Plantilla Report Recibos', 2),
        ('PILOT_2026_02_SEGURETXE.xlsx', 'Data', 2),
        ('PILOT_2026_01_ARRENTA.xlsx', 'Plantilla Report Recibos', 0),
        ('PILOT_202602_ARRENTA.xlsx', None, 0),  # First sheet
    ]
    
    for filename, sheet_name, header_row in pilot_files:
        fp = os.path.join(data_dir, filename)
        if not os.path.exists(fp):
            continue
        
        try:
            wb = openpyxl.load_workbook(fp, read_only=True)
            if sheet_name is None:
                sheet_name = wb.sheetnames[0]
            
            ws = wb[sheet_name]
            total_rows = ws.max_row or 0
            
            header_row_data = list(ws.iter_rows(min_row=header_row+1, max_row=header_row+1, values_only=True))
            headers = [str(h)[:25] for h in header_row_data[0] if h] if header_row_data else []
            
            wb.close()
            
            df = pd.read_excel(fp, sheet_name=sheet_name, header=header_row)
            
            file_info = {
                'filename': filename,
                'tipo': 'Pilot Data Quality',
                'pestaña_leida': sheet_name,
                'header_en_fila': header_row + 1,
                'total_filas_excel': total_rows,
                'total_filas_datos': len(df),
                'columnas_detectadas': headers[:12],
                'verificacion': f'Abre {filename}, pestaña "{sheet_name}". Verifica {len(df)} filas de datos.',
            }
            report['files'].append(file_info)
            
        except Exception as e:
            report['files'].append({'filename': filename, 'error': str(e)})
    
    # ── SUMMARY ──────────────────────────────────────────────────────────
    total_files = len(report['files'])
    files_ok = sum(1 for f in report['files'] if 'error' not in f)
    files_error = sum(1 for f in report['files'] if 'error' in f)
    
    report['summary'] = {
        'total_files': total_files,
        'files_ok': files_ok,
        'files_error': files_error,
        'instrucciones': (
            'Para verificar los datos:\n'
            '1. Abre cada fichero mencionado en Excel\n'
            '2. Ve a la pestaña indicada\n'
            '3. Verifica que el número de filas coincide\n'
            '4. Verifica que los totales de las columnas numéricas coinciden\n'
            '5. Compara las primeras filas mostradas con las del Excel\n'
            '6. Si todo coincide, los datos están correctamente extraídos.'
        ),
    }
    
    return report


def generate_spot_checks(data_dir):
    """
    Generate specific spot-check values that can be verified in 2 minutes.
    Returns a small set of values to manually verify in the original Excel files.
    """
    checks = []
    
    # Spot check 1: ELEVIA total rows
    fp = os.path.join(data_dir, '2026_02_ELEVIA.xlsx')
    if os.path.exists(fp):
        df = pd.read_excel(fp, sheet_name='Data')
        checks.append({
            'fichero': '2026_02_ELEVIA.xlsx → pestaña "Data"',
            'que_verificar': f'Número total de filas de datos: {len(df)}',
            'como': 'Abre el fichero, ve a "Data", selecciona la última fila con datos. Debe ser la fila ~{len(df)+1}.',
        })
        
        # Spot check 2: ELEVIA sum of a specific column
        if 'Comisión prima neta' in df.columns:
            total = pd.to_numeric(df['Comisión prima neta'], errors='coerce').sum()
            checks.append({
                'fichero': '2026_02_ELEVIA.xlsx → columna "Comisión prima neta"',
                'que_verificar': f'Suma total: {round(total, 2)}',
                'como': 'En Excel, al final de la columna "Comisión prima neta" pon =SUMA() de toda la columna.',
            })
    
    # Spot check 3: Saldos contables - specific empresa + cuenta
    fp2 = os.path.join(data_dir, 'Saldos_Contables_Ene_y_Feb_2026.xlsx')
    if os.path.exists(fp2):
        wb = openpyxl.load_workbook(fp2, read_only=True)
        if 'Ene-26' in wb.sheetnames:
            ws = wb['Ene-26']
            # Find Insurart 705000
            for row in ws.iter_rows(min_row=2, values_only=True):
                if row[0] and 'Insurart' in str(row[0]) and row[1] and str(row[1]).startswith('705'):
                    checks.append({
                        'fichero': 'Saldos_Contables → pestaña "Ene-26"',
                        'que_verificar': f'{str(row[0])[:35]} | Cuenta {row[1]} | Saldo: {round(float(row[3]), 2) if row[3] else 0}',
                        'como': 'Busca esta empresa y cuenta en la pestaña Ene-26. Verifica que el saldo coincide.',
                    })
                    break
        wb.close()
    
    # Spot check 4: Pilot file row count
    fp3 = os.path.join(data_dir, 'PILOT_202602_Araytor.xlsx')
    if os.path.exists(fp3):
        df3 = pd.read_excel(fp3, sheet_name='Plantilla Report Recibos', header=2)
        checks.append({
            'fichero': 'PILOT_202602_Araytor.xlsx → pestaña "Plantilla Report Recibos"',
            'que_verificar': f'Filas de datos: {len(df3)} (headers en fila 3)',
            'como': 'Abre el fichero. Los headers reales están en fila 3. Cuenta las filas de datos debajo.',
        })
    
    return {
        'titulo': 'Spot Checks — Verificación rápida (2 minutos)',
        'instrucciones': 'Verifica estos 4 valores abriendo los ficheros originales. Si todos coinciden, los datos están bien extraídos.',
        'checks': checks,
    }
