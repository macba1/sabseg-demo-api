"""
Generador de Log Detallado — Sabseg Data Quality
==================================================
Genera un log fila por fila de cada error detectado y cada corrección aplicada.
El log se puede mostrar en pantalla y exportar como Excel.
"""

import pandas as pd
import re
from io import BytesIO
from datetime import datetime
from data_quality import detect_structure, validate_nif, _find_column, _find_columns


def generate_detailed_log(file_bytes, filename):
    """
    Generate a row-by-row log of all errors and corrections for a file.
    Returns list of log entries, each with: fila, campo, valor_original, error, accion, valor_corregido
    """
    
    file_info = detect_structure(file_bytes, filename)
    if 'error' in file_info:
        return {'error': file_info['error'], 'log': []}
    
    df = file_info['dataframe']
    mapping = file_info.get('mapping', {})
    correduria = file_info.get('correduria', filename)
    
    log_entries = []
    
    # ── NIFs ──────────────────────────────────────────────────────────────
    nif_col = _find_column(df, ['NIF', 'Nif', 'nif', 'CIF'])
    if nif_col:
        for idx in df.index:
            nif_val = df.at[idx, nif_col]
            is_valid, error_type, suggestion = validate_nif(nif_val)
            if not is_valid:
                row_num = idx + 2  # +2 for header row + 0-index
                if error_type == 'vacio':
                    log_entries.append({
                        'fila': row_num, 'campo': nif_col,
                        'valor_original': '', 'error': 'NIF vacío',
                        'accion': 'Corregido', 'valor_corregido': 'X0000000T',
                        'tipo': 'error', 'auto_fix': True,
                    })
                elif error_type == 'sin_letra':
                    num = str(nif_val).strip()
                    from data_quality import NIF_LETTER_TABLE
                    if num.isdigit() and len(num) == 8:
                        letter = NIF_LETTER_TABLE[int(num) % 23]
                        log_entries.append({
                            'fila': row_num, 'campo': nif_col,
                            'valor_original': str(nif_val), 'error': 'NIF sin letra',
                            'accion': 'Corregido', 'valor_corregido': f'{num}{letter}',
                            'tipo': 'error', 'auto_fix': True,
                        })
                    else:
                        log_entries.append({
                            'fila': row_num, 'campo': nif_col,
                            'valor_original': str(nif_val), 'error': f'NIF {error_type}',
                            'accion': 'Requiere revisión', 'valor_corregido': suggestion or '',
                            'tipo': 'error', 'auto_fix': False,
                        })
                elif error_type in ('letra_incorrecta', 'nie_letra_incorrecta'):
                    corrected = suggestion.split(':')[-1].strip() if suggestion and ':' in suggestion else ''
                    log_entries.append({
                        'fila': row_num, 'campo': nif_col,
                        'valor_original': str(nif_val), 'error': f'NIF letra incorrecta',
                        'accion': 'Corregido' if corrected else 'Requiere revisión',
                        'valor_corregido': corrected,
                        'tipo': 'error', 'auto_fix': bool(corrected),
                    })
                elif error_type == 'caracteres_especiales':
                    cleaned = re.sub(r'[\s\.\-,/]', '', str(nif_val).strip()).upper()
                    log_entries.append({
                        'fila': row_num, 'campo': nif_col,
                        'valor_original': str(nif_val), 'error': 'NIF con caracteres especiales',
                        'accion': 'Corregido', 'valor_corregido': cleaned,
                        'tipo': 'error', 'auto_fix': True,
                    })
                else:
                    log_entries.append({
                        'fila': row_num, 'campo': nif_col,
                        'valor_original': str(nif_val), 'error': f'NIF {error_type}',
                        'accion': 'Requiere revisión', 'valor_corregido': suggestion or '',
                        'tipo': 'error', 'auto_fix': False,
                    })
    
    # ── Descuadre comisiones CB ≠ CPN + CC ────────────────────────────────
    cb_col = _find_column(df, ['ComisionCorreduria', 'Com, Bruta', 'Comisión Bruta'])
    cpn_col = _find_column(df, ['ComisionPrimaNeta', 'Comisión prima neta', 'Comisión Prima Neta'])
    cc_col = _find_column(df, ['ComisionComplementaria', 'Comisión complementaria'])
    
    if cb_col and cpn_col and cc_col:
        cb = pd.to_numeric(df[cb_col], errors='coerce').fillna(0)
        cpn = pd.to_numeric(df[cpn_col], errors='coerce').fillna(0)
        cc = pd.to_numeric(df[cc_col], errors='coerce').fillna(0)
        diff = abs(cb - (cpn + cc))
        
        for idx in df.index:
            if diff.at[idx] > 0.01:
                row_num = idx + 2
                expected = round(cpn.at[idx] + cc.at[idx], 2)
                log_entries.append({
                    'fila': row_num, 'campo': cb_col,
                    'valor_original': f'{cb.at[idx]:.2f} (CPN={cpn.at[idx]:.2f} + CC={cc.at[idx]:.2f} = {expected:.2f})',
                    'error': 'Descuadre CB ≠ CPN + CC',
                    'accion': 'Corregido', 'valor_corregido': f'{expected:.2f}',
                    'tipo': 'error', 'auto_fix': True,
                })
    
    # ── Productos no mapeados (agrupados por producto único) ─────────────
    if 'ramo_mapping' in mapping:
        producto_col = _find_column(df, ['Producto', 'Ramo'])
        if producto_col:
            known = set(mapping['ramo_mapping'].keys())
            unmapped_products = {}
            for idx in df.index:
                val = df.at[idx, producto_col]
                if pd.notna(val) and str(val).strip() not in known:
                    prod_name = str(val).strip()
                    if prod_name not in unmapped_products:
                        unmapped_products[prod_name] = {'count': 0, 'first_row': idx + 2}
                    unmapped_products[prod_name]['count'] += 1
            
            for prod_name, info in sorted(unmapped_products.items()):
                log_entries.append({
                    'fila': info['first_row'], 'campo': producto_col,
                    'valor_original': prod_name,
                    'error': f'Producto no mapeado ({info["count"]} recibos)',
                    'accion': 'Requiere revisión', 'valor_corregido': '',
                    'tipo': 'error', 'auto_fix': False,
                })
    
    # ── Espacios en blanco ────────────────────────────────────────────────
    for col in df.columns:
        if df[col].dtype == object:
            for idx in df.index:
                val = df.at[idx, col]
                if pd.notna(val):
                    s = str(val)
                    if s != s.strip() and s.strip():
                        row_num = idx + 2
                        log_entries.append({
                            'fila': row_num, 'campo': col,
                            'valor_original': f'"{s}"', 'error': 'Espacios en blanco',
                            'accion': 'Corregido', 'valor_corregido': f'"{s.strip()}"',
                            'tipo': 'correccion', 'auto_fix': True,
                        })
    
    # ── Fecha anulación < fecha emisión ───────────────────────────────────
    fecha_fac_col = _find_column(df, ['FechaFacturacion', 'F. Producc.'])
    anulacion_col = _find_column(df, ['FechaAnulacion'])
    if fecha_fac_col and anulacion_col:
        f_fac = pd.to_datetime(df[fecha_fac_col], errors='coerce')
        f_anu = pd.to_datetime(df[anulacion_col], errors='coerce')
        for idx in df.index:
            if pd.notna(f_anu.at[idx]) and pd.notna(f_fac.at[idx]) and f_anu.at[idx] < f_fac.at[idx]:
                row_num = idx + 2
                log_entries.append({
                    'fila': row_num, 'campo': anulacion_col,
                    'valor_original': f'Anulación: {f_anu.at[idx].strftime("%d/%m/%Y")}, Emisión: {f_fac.at[idx].strftime("%d/%m/%Y")}',
                    'error': 'Fecha anulación anterior a emisión',
                    'accion': 'Requiere revisión', 'valor_corregido': '',
                    'tipo': 'warning', 'auto_fix': False,
                })
    
    # ── Comisión 0 con prima > 0 ──────────────────────────────────────────
    prima_col = _find_column(df, ['PrimaNeta', 'Prima neta', 'Prima'])
    com_col = _find_column(df, ['ComisionCorreduria', 'ComisionPrimaNeta', 'Com, Bruta'])
    if prima_col and com_col:
        prima = pd.to_numeric(df[prima_col], errors='coerce').fillna(0)
        com = pd.to_numeric(df[com_col], errors='coerce').fillna(0)
        for idx in df.index:
            if prima.at[idx] > 0 and com.at[idx] == 0:
                row_num = idx + 2
                log_entries.append({
                    'fila': row_num, 'campo': com_col,
                    'valor_original': f'Comisión=0, Prima={prima.at[idx]:.2f}',
                    'error': 'Comisión 0 con prima positiva',
                    'accion': 'Requiere revisión', 'valor_corregido': '',
                    'tipo': 'warning', 'auto_fix': False,
                })
    
    # Sort by row number
    log_entries.sort(key=lambda x: x['fila'])
    
    # Summary
    total = len(log_entries)
    auto_fixed = sum(1 for e in log_entries if e['auto_fix'])
    needs_review = total - auto_fixed
    
    return {
        'filename': filename,
        'correduria': correduria,
        'total_registros': len(df),
        'total_incidencias': total,
        'auto_corregidas': auto_fixed,
        'requieren_revision': needs_review,
        'log': log_entries,
    }


def generate_log_excel(log_result):
    """Convert log to downloadable Excel bytes."""
    if not log_result.get('log'):
        return None
    
    df_log = pd.DataFrame(log_result['log'])
    df_log = df_log[['fila', 'campo', 'valor_original', 'error', 'accion', 'valor_corregido', 'tipo']]
    df_log.columns = ['Fila', 'Campo', 'Valor Original', 'Error', 'Acción', 'Valor Corregido', 'Tipo']
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_log.to_excel(writer, index=False, sheet_name='Log de Errores')
        
        # Summary sheet
        summary = pd.DataFrame([{
            'Fichero': log_result['filename'],
            'Correduría': log_result['correduria'],
            'Total registros': log_result['total_registros'],
            'Total incidencias': log_result['total_incidencias'],
            'Auto-corregidas': log_result['auto_corregidas'],
            'Requieren revisión': log_result['requieren_revision'],
            'Fecha análisis': datetime.now().strftime('%d/%m/%Y %H:%M'),
        }])
        summary.to_excel(writer, index=False, sheet_name='Resumen')
    
    return output.getvalue()


def generate_all_logs(files):
    """Generate logs for multiple files."""
    results = []
    for filename, file_bytes in files:
        result = generate_detailed_log(file_bytes, filename)
        results.append(result)
    
    total_incidencias = sum(r.get('total_incidencias', 0) for r in results if 'error' not in r)
    total_auto = sum(r.get('auto_corregidas', 0) for r in results if 'error' not in r)
    total_review = sum(r.get('requieren_revision', 0) for r in results if 'error' not in r)
    
    return {
        'fecha': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'total_ficheros': len(results),
        'total_incidencias': total_incidencias,
        'auto_corregidas': total_auto,
        'requieren_revision': total_review,
        'ficheros': results,
    }
