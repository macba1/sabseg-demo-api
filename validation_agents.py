"""
Validation Agents — Phase 1: FileReader + DataVerifier
=======================================================
Two-agent pipeline that reads Excel/PDF files at the raw level (openpyxl)
and produces structured validation reports with spot checks.
"""

import os
import re
from datetime import datetime

import openpyxl
import pandas as pd


# Keywords that indicate a header row
HEADER_KEYWORDS = [
    'nif', 'poliza', 'póliza', 'prima', 'comisión', 'comision',
    'correduria', 'correduría', 'recibo', 'fecha', 'producto',
    'compañia', 'compania', 'importe', 'asegurado', 'tomador',
]


class FileReader:
    """
    Agent 1 — Reads an Excel file at the raw level using openpyxl.
    Detects sheets, header row, real data rows, column issues.
    """

    def read_file(self, filepath):
        """Read file and return structured analysis."""
        filename = os.path.basename(filepath)
        print(f"[FileReader] Reading {filename}")

        if filepath.lower().endswith('.pdf'):
            return self._read_pdf(filepath, filename)

        try:
            wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        except Exception as e:
            print(f"[FileReader] ERROR opening {filename}: {e}")
            return {
                'filename': filename,
                'status': 'error',
                'error': f'No se pudo abrir el fichero: {str(e)}',
                'sheets': [],
                'alerts': [f'Error al abrir: {str(e)}'],
            }

        sheets_info = []
        best_sheet = None
        best_row_count = 0

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            try:
                max_row = ws.max_row or 0
                max_col = ws.max_column or 0
            except Exception:
                max_row = 0
                max_col = 0

            # Estimate non-empty rows from first 200 rows
            non_empty = 0
            sample_limit = min(max_row, 200)
            for row in ws.iter_rows(min_row=1, max_row=sample_limit, max_col=min(max_col, 30), values_only=True):
                if row and any(cell is not None and str(cell).strip() != '' for cell in row):
                    non_empty += 1
            # Extrapolate if sampled
            if max_row > sample_limit and non_empty > 0:
                non_empty = int(non_empty / sample_limit * max_row)

            info = {
                'name': sheet_name,
                'max_rows': max_row,
                'max_cols': max_col,
                'non_empty_rows': non_empty,
            }
            sheets_info.append(info)

            if non_empty > best_row_count:
                best_row_count = non_empty
                best_sheet = sheet_name

        if not best_sheet:
            wb.close()
            return {
                'filename': filename,
                'status': 'error',
                'error': 'No se encontró ninguna pestaña con datos',
                'sheets': sheets_info,
                'alerts': ['Todas las pestañas están vacías'],
            }

        # Analyze best sheet in detail
        ws = wb[best_sheet]
        max_row = ws.max_row or 0
        max_col = ws.max_column or 0

        # Find header row
        header_row_num = None
        header_cells = []
        for row_idx in range(1, min(max_row + 1, 30)):
            row_vals = []
            for col_idx in range(1, min(max_col + 1, 100)):
                val = ws.cell(row=row_idx, column=col_idx).value
                row_vals.append(str(val).strip() if val is not None else '')

            non_empty_cells = [v for v in row_vals if v]
            if len(non_empty_cells) < 3:
                continue

            # Check if this row looks like a header
            lower_vals = ' '.join(non_empty_cells).lower()
            matches = sum(1 for kw in HEADER_KEYWORDS if kw in lower_vals)
            if matches >= 2:
                header_row_num = row_idx
                header_cells = row_vals
                break

        if header_row_num is None:
            # Fallback: use first row with 5+ non-empty cells
            for row_idx in range(1, min(max_row + 1, 15)):
                row_vals = []
                for col_idx in range(1, min(max_col + 1, 100)):
                    val = ws.cell(row=row_idx, column=col_idx).value
                    row_vals.append(str(val).strip() if val is not None else '')
                non_empty_cells = [v for v in row_vals if v]
                if len(non_empty_cells) >= 5:
                    header_row_num = row_idx
                    header_cells = row_vals
                    break

        # Extract column names (trim trailing empty)
        columns = []
        if header_cells:
            last_non_empty = 0
            for i, v in enumerate(header_cells):
                if v:
                    last_non_empty = i
            columns = header_cells[:last_non_empty + 1]

        # Detect duplicate columns
        col_lower = [c.lower().strip() for c in columns if c]
        seen = {}
        duplicate_cols = []
        for c in col_lower:
            if c in seen:
                duplicate_cols.append(c)
            seen[c] = True

        # Use pandas for efficient row counting and data extraction
        wb.close()

        data_start = (header_row_num or 1) + 1
        real_rows = 0
        empty_tail = 0
        first_row_data = None
        last_row_data = None

        try:
            df_raw = pd.read_excel(filepath, sheet_name=best_sheet,
                                   header=header_row_num - 1 if header_row_num else 0,
                                   engine='openpyxl')
            # Drop fully empty rows
            df_nonull = df_raw.dropna(how='all')
            real_rows = len(df_nonull)
            empty_tail = len(df_raw) - len(df_raw.loc[:df_nonull.index[-1]]) if len(df_nonull) > 0 else len(df_raw)

            if real_rows > 0:
                first_row_data = df_nonull.iloc[0].to_dict()
                last_row_data = df_nonull.iloc[-1].to_dict()
        except Exception as e:
            print(f"[FileReader] Pandas fallback failed: {e}")
            real_rows = max(best_row_count - 1, 0)  # estimate

        # Build alerts
        alerts = []
        if len(wb.sheetnames if hasattr(wb, 'sheetnames') else sheets_info) > 1:
            other = [s['name'] for s in sheets_info if s['name'] != best_sheet]
            alerts.append(f"Fichero con múltiples pestañas. Se usó '{best_sheet}'. Otras: {', '.join(other)}")
        if duplicate_cols:
            alerts.append(f"Columnas duplicadas detectadas: {', '.join(set(duplicate_cols))}")
        if empty_tail > 10:
            alerts.append(f"{empty_tail} filas vacías al final del fichero")
        if header_row_num and header_row_num > 1:
            alerts.append(f"La cabecera no está en la fila 1 (encontrada en fila {header_row_num})")
        if real_rows == 0:
            alerts.append("No se encontraron registros de datos reales")

        # Serialize first/last rows
        def serialize_row(row_dict):
            if not row_dict:
                return None
            result = {}
            for k, v in row_dict.items():
                if v is None:
                    result[k] = None
                elif isinstance(v, datetime):
                    result[k] = v.isoformat()
                else:
                    result[k] = str(v) if not isinstance(v, (int, float, bool)) else v
            return result

        print(f"[FileReader] {filename}: sheet='{best_sheet}', header_row={header_row_num}, "
              f"columns={len(columns)}, rows={real_rows}, alerts={len(alerts)}")

        return {
            'filename': filename,
            'status': 'ok',
            'sheets': sheets_info,
            'selected_sheet': best_sheet,
            'header_row': header_row_num,
            'columns': columns,
            'column_count': len(columns),
            'duplicate_columns': list(set(duplicate_cols)),
            'total_rows': real_rows,
            'empty_tail_rows': empty_tail,
            'first_row': serialize_row(first_row_data),
            'last_row': serialize_row(last_row_data),
            'alerts': alerts,
        }

    def _read_pdf(self, filepath, filename):
        """Minimal PDF handling — flag as non-tabular."""
        print(f"[FileReader] {filename} is a PDF — limited analysis")
        return {
            'filename': filename,
            'status': 'warning',
            'error': 'Fichero PDF: análisis limitado, se recomienda Excel',
            'sheets': [],
            'selected_sheet': None,
            'header_row': None,
            'columns': [],
            'column_count': 0,
            'duplicate_columns': [],
            'total_rows': 0,
            'empty_tail_rows': 0,
            'first_row': None,
            'last_row': None,
            'alerts': ['Los ficheros PDF tienen análisis limitado. Se recomienda enviar en formato Excel.'],
        }


class DataVerifier:
    """
    Agent 2 — Runs spot checks and anomaly detection on the reader result.
    """

    def verify_file(self, filepath, reader_result):
        """Generate spot checks and anomaly detection."""
        filename = reader_result.get('filename', os.path.basename(filepath))
        print(f"[DataVerifier] Verifying {filename}")

        if reader_result.get('status') == 'error':
            return {
                'filename': filename,
                'status': 'skipped',
                'reason': reader_result.get('error', 'Reader failed'),
                'spot_checks': [],
                'anomalies': [],
            }

        spot_checks = []
        anomalies = []

        total_rows = reader_result.get('total_rows', 0)
        columns = reader_result.get('columns', [])
        first_row = reader_result.get('first_row')
        last_row = reader_result.get('last_row')

        # Spot check 1: Record count
        spot_checks.append({
            'check': 'Conteo de registros',
            'value': total_rows,
            'detail': f'{total_rows} registros de datos reales (excluyendo cabecera y filas vacías)',
        })

        # Spot check 2: First row sample (3 fields)
        if first_row and isinstance(first_row, dict):
            sample = {k: v for k, v in list(first_row.items())[:3]}
            spot_checks.append({
                'check': 'Primera fila (3 campos)',
                'value': sample,
                'detail': 'Primeros 3 campos del primer registro',
            })

        # Spot check 3: Last row sample (2 fields)
        if last_row and isinstance(last_row, dict):
            sample = {k: v for k, v in list(last_row.items())[:2]}
            spot_checks.append({
                'check': 'Última fila (2 campos)',
                'value': sample,
                'detail': 'Primeros 2 campos del último registro',
            })

        # For deeper checks, read with pandas
        header_row = reader_result.get('header_row')
        selected_sheet = reader_result.get('selected_sheet')

        if filepath.lower().endswith('.pdf') or not header_row or not selected_sheet:
            return {
                'filename': filename,
                'status': 'partial',
                'spot_checks': spot_checks,
                'anomalies': anomalies,
            }

        try:
            df = pd.read_excel(filepath, sheet_name=selected_sheet,
                               header=header_row - 1, engine='openpyxl')
        except Exception as e:
            print(f"[DataVerifier] Pandas read failed for {filename}: {e}")
            return {
                'filename': filename,
                'status': 'partial',
                'spot_checks': spot_checks,
                'anomalies': [f'No se pudo leer con pandas: {str(e)}'],
            }

        # Spot check 4: Sum of main numeric column
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        prima_col = None
        for c in numeric_cols:
            cl = c.lower() if isinstance(c, str) else ''
            if any(kw in cl for kw in ['prima', 'importe', 'comisi']):
                prima_col = c
                break
        if not prima_col and numeric_cols:
            prima_col = numeric_cols[0]

        if prima_col:
            col_sum = pd.to_numeric(df[prima_col], errors='coerce').sum()
            spot_checks.append({
                'check': f'Suma columna "{prima_col}"',
                'value': round(float(col_sum), 2) if pd.notna(col_sum) else 0,
                'detail': f'Suma total de la columna numérica principal',
            })

        # Spot check 5: Unique values in main text column
        text_cols = df.select_dtypes(include=['object']).columns.tolist()
        main_text = None
        for c in text_cols:
            cl = c.lower() if isinstance(c, str) else ''
            if any(kw in cl for kw in ['poliza', 'póliza', 'nif', 'asegurado', 'tomador']):
                main_text = c
                break
        if not main_text and text_cols:
            main_text = text_cols[0]

        if main_text:
            nunique = df[main_text].nunique()
            spot_checks.append({
                'check': f'Valores únicos en "{main_text}"',
                'value': int(nunique),
                'detail': f'{nunique} valores distintos de {len(df)} registros',
            })

        # Anomaly detection
        if total_rows == 0:
            anomalies.append({
                'type': 'critical',
                'message': '0 registros de datos — fichero vacío o sin datos válidos',
            })

        # Columns 100% empty
        for col in df.columns:
            if df[col].isna().all() or (df[col].astype(str).str.strip() == '').all():
                anomalies.append({
                    'type': 'warning',
                    'message': f'Columna "{col}" está 100% vacía',
                })

        # All values identical in a column (suspicious)
        for col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) > 10 and non_null.nunique() == 1:
                anomalies.append({
                    'type': 'warning',
                    'message': f'Columna "{col}" tiene todos los valores iguales: {non_null.iloc[0]}',
                })

        # Duplicate columns from reader
        dup_cols = reader_result.get('duplicate_columns', [])
        if dup_cols:
            anomalies.append({
                'type': 'warning',
                'message': f'Columnas con nombres duplicados: {", ".join(dup_cols)}',
            })

        # Empty tail rows
        empty_tail = reader_result.get('empty_tail_rows', 0)
        if empty_tail > 50:
            anomalies.append({
                'type': 'info',
                'message': f'{empty_tail} filas vacías al final del fichero (posible basura)',
            })

        print(f"[DataVerifier] {filename}: {len(spot_checks)} checks, {len(anomalies)} anomalies")

        return {
            'filename': filename,
            'status': 'ok',
            'spot_checks': spot_checks,
            'anomalies': anomalies,
        }


# ── Format signatures for reconciliation files ───────────────────────────────

FORMAT_SIGNATURES = [
    {
        'name': 'ELEVIA',
        'match': ['Comisión prima neta', 'Comisión complementaria', 'Comisión correduría', 'Comisión Colaborador', 'Empresa'],
        'min_match': 4,
        'col_705': ['Comisión prima neta', 'Comisión complementaria'],
        'formula_705': 'CPN + CC (o Comisión correduría)',
        'col_623': ['Comisión Colaborador'],
        'formula_623': 'Comisión Colaborador',
    },
    {
        'name': 'PLANTILLA_REPORT',
        'match': ['ComisionCorreduria', 'ComisionPrimaNeta', 'ComisionComplementaria', 'ComisionColaborador1'],
        'min_match': 3,
        'col_705': ['ComisionPrimaNeta', 'ComisionComplementaria'],
        'formula_705': 'CPN + CC (= ComisionCorreduria)',
        'col_623': ['ComisionColaborador1', 'ComisionColaborador2', 'ComisionColaborador3', 'ComisionColaboradorSupervisor'],
        'formula_623': 'Suma Colaboradores',
    },
    {
        'name': 'MODELO_DATOS',
        'match': ['Comisión prima neta', 'Comisión complementaria', 'Comisión Bruta', 'Colaborador 1'],
        'min_match': 3,
        'col_705': ['Comisión prima neta', 'Comisión complementaria'],
        'formula_705': 'CPN + CC (= Comisión Bruta)',
        'col_623': ['Comisión colaborador 1', 'Comisión colaborador 2', 'Comisión colaborador 3'],
        'formula_623': 'Suma Comisión colaboradores',
    },
    {
        'name': 'FUTURA',
        'match': ['Comisión neta €', 'Comisión cedida €', 'Comisión Bruta\n(Cedida + Neta + Honorarios)'],
        'min_match': 2,
        'col_705': ['Comisión neta €'],
        'formula_705': 'Comisión neta € (+ Honorarios?)',
        'col_623': ['Comisión cedida €'],
        'formula_623': 'Comisión cedida €',
        'extra': {'honorarios': 'Prima total \n(Neta + Honorarios)'},
        'alert': 'Fichero tiene Honorarios separados. No incluidos en 705 por defecto. Confirmar con cliente.',
    },
    {
        'name': 'SEGURETXE',
        'match': ['Com, Bruta', 'Comisión Prima Neta', 'Sobrecomisión', 'Comisión Cedida al colaborador'],
        'min_match': 3,
        'col_705': ['Comisión Prima Neta', 'Sobrecomisión'],
        'formula_705': 'CPN + Sobrecomisión (= Com Bruta)',
        'col_623': ['Comisión Cedida al colaborador'],
        'formula_623': 'Comisión Cedida al colaborador',
    },
    {
        'name': 'SANCHEZ_VALENCIA',
        'match': ['Comisión correduría', 'Comisión prima neta', 'Comisión complementaria', 'Comisión colaborador'],
        'min_match': 3,
        'col_705': ['Comisión prima neta', 'Comisión complementaria'],
        'formula_705': 'CPN + CC (= Comisión correduría)',
        'col_623': ['Comisión colaborador'],
        'formula_623': 'Comisión colaborador',
    },
    {
        'name': 'AGRO',
        'match': ['RCom.Bruta', 'RCom.Neta', 'RCom.Cedida', 'RP.Neta', 'RHonorarios'],
        'min_match': 3,
        'col_705': ['RCom.Neta', 'RHonorarios'],
        'formula_705': 'RCom.Neta + RHonorarios (ALERTA: confirmar)',
        'col_623': ['RCom.Cedida', 'RCom.CedidaReal'],
        'formula_623': 'RCom.Cedida / RCom.CedidaReal',
        'alert': 'Formato AGRO con múltiples columnas de comisión. Requiere revisión manual de mapping.',
    },
    {
        'name': 'HONORARIOS',
        'match': ['COLABORADOR 1', 'IMP. COLAB.', '% COMIS.'],
        'min_match': 2,
        'col_705': [],
        'formula_705': 'N/A — fichero de honorarios, no contiene comisiones 705',
        'col_623': ['IMP. COLAB.', 'IMP. COLAB.2'],
        'formula_623': 'Importes colaboradores',
        'alert': 'Fichero de honorarios. Solo contiene comisiones cedidas (623), no comisiones propias (705).',
    },
]


class FormatDetector:
    """
    Agent 3 — Detects the format/structure of a reconciliation file
    and identifies which columns map to cuenta 705 and 623.
    """

    def detect_format(self, filepath, reader_result):
        """Identify file format and commission column mapping."""
        filename = reader_result.get('filename', os.path.basename(filepath))
        print(f"[FormatDetector] Analyzing {filename}")

        if reader_result.get('status') == 'error':
            return {
                'filename': filename,
                'formato': 'ERROR',
                'confianza': 'ninguna',
                'columnas_705': [],
                'columnas_623': [],
                'formula_705_propuesta': 'N/A',
                'formula_623_propuesta': 'N/A',
                'alertas': [reader_result.get('error', 'Error en lectura')],
                'todas_columnas_comision': [],
            }

        if filepath.lower().endswith('.pdf'):
            return {
                'filename': filename,
                'formato': 'PDF',
                'confianza': 'baja',
                'columnas_705': [],
                'columnas_623': [],
                'formula_705_propuesta': 'Extracción de texto PDF necesaria',
                'formula_623_propuesta': 'Extracción de texto PDF necesaria',
                'alertas': ['Fichero PDF. Se requiere pdfplumber para extracción de datos.'],
                'todas_columnas_comision': [],
            }

        columns = reader_result.get('columns', [])
        col_lower_map = {}  # lower → original
        for c in columns:
            if c:
                col_lower_map[c.lower().strip()] = c

        # Find all commission-related columns
        comision_keywords = ['comis', 'honor', 'cedid', 'colab', 'brut', 'neta', 'sobr', 'imp. colab']
        todas_comision = [c for c in columns if c and any(kw in c.lower() for kw in comision_keywords)]

        # Try to match against known signatures
        best_format = None
        best_score = 0

        for sig in FORMAT_SIGNATURES:
            score = 0
            for pattern in sig['match']:
                pattern_lower = pattern.lower().strip()
                # Check exact match or substring match
                for col in columns:
                    if col and (col.lower().strip() == pattern_lower or pattern_lower in col.lower()):
                        score += 1
                        break
            if score >= sig['min_match'] and score > best_score:
                best_score = score
                best_format = sig

        if best_format:
            # Find actual column names for the mapped columns
            def find_actual(candidates):
                found = []
                for cand in candidates:
                    cand_lower = cand.lower().strip()
                    for col in columns:
                        if col and (col.lower().strip() == cand_lower or cand_lower in col.lower()):
                            found.append(col)
                            break
                return found

            col_705 = find_actual(best_format['col_705'])
            col_623 = find_actual(best_format['col_623'])
            confianza = 'alta' if best_score >= len(best_format['match']) else 'media'

            alertas = []
            if best_format.get('alert'):
                alertas.append(best_format['alert'])
            if not col_705:
                alertas.append('No se encontraron columnas para cuenta 705')
                confianza = 'baja'
            if not col_623:
                alertas.append('No se encontraron columnas para cuenta 623')

            extra = {}
            if best_format.get('extra'):
                for key, pattern in best_format['extra'].items():
                    actual = find_actual([pattern])
                    if actual:
                        extra[key] = actual[0]

            result = {
                'filename': filename,
                'formato': best_format['name'],
                'confianza': confianza,
                'columnas_705': col_705,
                'columnas_623': col_623,
                'columnas_adicionales': extra if extra else None,
                'formula_705_propuesta': best_format['formula_705'],
                'formula_623_propuesta': best_format['formula_623'],
                'alertas': alertas,
                'todas_columnas_comision': todas_comision,
            }
        else:
            # Unknown format
            result = {
                'filename': filename,
                'formato': 'DESCONOCIDO',
                'confianza': 'baja',
                'columnas_705': [],
                'columnas_623': [],
                'columnas_adicionales': None,
                'formula_705_propuesta': 'No se pudo detectar automáticamente',
                'formula_623_propuesta': 'No se pudo detectar automáticamente',
                'alertas': [
                    f'Formato no reconocido. {len(columns)} columnas detectadas.',
                    f'Columnas con posible comisión: {", ".join(todas_comision) if todas_comision else "ninguna"}',
                ],
                'todas_columnas_comision': todas_comision,
            }

        print(f"[FormatDetector] {filename}: formato={result['formato']}, "
              f"confianza={result['confianza']}, 705={result['columnas_705']}, 623={result['columnas_623']}")

        return result


# ── Pipeline orchestrators ────────────────────────────────────────────────────

def validate_files(file_paths):
    """Run FileReader + DataVerifier on a list of file paths."""
    reader = FileReader()
    verifier = DataVerifier()

    results = []
    for fp in file_paths:
        reader_result = reader.read_file(fp)
        verifier_result = verifier.verify_file(fp, reader_result)
        results.append({
            'reader': reader_result,
            'verifier': verifier_result,
        })

    return {
        'timestamp': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'total_files': len(results),
        'files': results,
    }


def validate_recon_files(file_paths):
    """Run FileReader + DataVerifier + FormatDetector on reconciliation files."""
    reader = FileReader()
    verifier = DataVerifier()
    detector = FormatDetector()

    results = []
    for fp in file_paths:
        reader_result = reader.read_file(fp)
        verifier_result = verifier.verify_file(fp, reader_result)
        format_result = detector.detect_format(fp, reader_result)
        results.append({
            'reader': reader_result,
            'verifier': verifier_result,
            'format': format_result,
        })

    return {
        'timestamp': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'total_files': len(results),
        'files': results,
    }
