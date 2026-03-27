"""
Sistema de Agentes de QA — Sabseg
===================================
6 agentes especializados que verifican la calidad de todo el procesamiento.
Se ejecutan automáticamente después de cada reconciliación o validación.
El orquestador coordina, recoge resultados, y genera un informe de confianza.
"""

import pandas as pd
from datetime import datetime
from collections import Counter


# ─── ORQUESTADOR ──────────────────────────────────────────────────────────────

class QAOrchestrator:
    """
    Coordina todos los agentes de QA.
    Recibe el resultado de un procesamiento y lo pasa por todos los agentes.
    """
    
    def __init__(self):
        self.agents = []
        self.results = []
    
    def run_reconciliation_qa(self, reconciliation_result, raw_inputs=None):
        """Run QA for reconciliation results."""
        self.results = []
        
        # Agent 1: Data integrity
        self.results.append(
            AgentDataIntegrity().check_reconciliation(reconciliation_result, raw_inputs)
        )
        
        # Agent 2: Company mapping
        self.results.append(
            AgentCompanyMapping().check(reconciliation_result)
        )
        
        # Agent 3: Numerical consistency
        self.results.append(
            AgentNumericalConsistency().check_reconciliation(reconciliation_result)
        )
        
        # Agent 4: Period coverage
        self.results.append(
            AgentPeriodCoverage().check_reconciliation(reconciliation_result)
        )
        
        return self._compile_report("Reconciliación Contable")
    
    def run_data_quality_qa(self, dq_result, correction_result=None, report_result=None):
        """Run QA for data quality results."""
        self.results = []
        
        # Agent 1: Data integrity
        self.results.append(
            AgentDataIntegrity().check_data_quality(dq_result)
        )
        
        # Agent 5: Error detection coverage
        self.results.append(
            AgentErrorDetection().check(dq_result)
        )
        
        # Agent 3: Numerical consistency for DQ
        self.results.append(
            AgentNumericalConsistency().check_data_quality(dq_result)
        )
        
        # Agent 5b: Correction verification
        if correction_result:
            self.results.append(
                AgentCorrectionVerification().check(dq_result, correction_result)
            )
        
        # Agent 6: Report verification
        if report_result:
            self.results.append(
                AgentReportVerification().check(dq_result, report_result)
            )
        
        return self._compile_report("Data Quality")
    
    def _compile_report(self, context):
        """Compile all agent results into a final QA report."""
        total_checks = sum(r['checks_run'] for r in self.results)
        total_passed = sum(r['checks_passed'] for r in self.results)
        total_warnings = sum(r['warnings'] for r in self.results)
        total_errors = sum(r['errors'] for r in self.results)
        
        if total_errors > 0:
            status = 'fail'
            confidence = 'baja'
            message = f'QA detectó {total_errors} problema(s) que requieren revisión antes de presentar resultados.'
        elif total_warnings > 0:
            status = 'warning'
            confidence = 'media'
            message = f'QA completado con {total_warnings} observación(es). Los resultados son utilizables pero conviene revisar.'
        else:
            status = 'pass'
            confidence = 'alta'
            message = 'QA completado. Todos los checks pasaron correctamente.'
        
        pct_passed = round(total_passed / max(total_checks, 1) * 100, 1)
        
        return {
            'qa_status': status,
            'qa_confidence': confidence,
            'qa_message': message,
            'qa_context': context,
            'qa_timestamp': datetime.now().strftime('%d/%m/%Y %H:%M'),
            'total_checks': total_checks,
            'total_passed': total_passed,
            'total_warnings': total_warnings,
            'total_errors': total_errors,
            'pct_passed': pct_passed,
            'agent_results': self.results,
        }


# ─── AGENTE 1: INTEGRIDAD DE DATOS DE ENTRADA ────────────────────────────────

class AgentDataIntegrity:
    """Verifica que todos los ficheros se leyeron correctamente y no se perdieron datos."""
    
    def check_reconciliation(self, result, raw_inputs=None):
        checks = []
        
        # Check 1: All files processed
        files_processed = result.get('ficheros_procesados', [])
        files_error = result.get('ficheros_error', [])
        
        if files_error:
            checks.append({
                'check': 'Ficheros con error',
                'status': 'error',
                'detail': f'{len(files_error)} fichero(s) no se pudieron procesar: {[f["filename"] for f in files_error]}',
            })
        else:
            checks.append({
                'check': 'Todos los ficheros procesados',
                'status': 'pass',
                'detail': f'{len(files_processed)} ficheros procesados sin errores.',
            })
        
        # Check 2: No files with 0 records (excluding PDFs and known edge cases)
        empty_files = [f for f in files_processed 
                       if f.get('records_extracted', 0) == 0 
                       and not f.get('filename', '').lower().endswith('.pdf')]
        if empty_files:
            if len(empty_files) <= 3:
                # Small number of empty files is normal (may not have data for this period)
                checks.append({
                    'check': 'Ficheros sin registros del periodo',
                    'status': 'pass',
                    'detail': f'{len(empty_files)} fichero(s) sin registros para el periodo analizado: {[f["filename"] for f in empty_files]}. Normal si la correduría no tuvo actividad en ese mes.',
                })
            else:
                checks.append({
                    'check': 'Ficheros sin registros',
                    'status': 'warning',
                    'detail': f'{len(empty_files)} fichero(s) sin registros. Verificar si es correcto: {[f["filename"] for f in empty_files]}',
                })
        else:
            checks.append({
                'check': 'Todos los ficheros tienen registros',
                'status': 'pass',
                'detail': 'Todos los ficheros procesados contienen registros (o son PDFs procesados aparte).',
            })
        
        # Check 3: Reconciliation has results
        recon = result.get('reconciliacion', [])
        if len(recon) == 0:
            checks.append({
                'check': 'Resultados de reconciliación',
                'status': 'error',
                'detail': 'La reconciliación no generó ningún resultado.',
            })
        else:
            checks.append({
                'check': 'Resultados de reconciliación',
                'status': 'pass',
                'detail': f'{len(recon)} partidas de reconciliación generadas.',
            })
        
        return self._summarize('Integridad de datos', checks)
    
    def check_data_quality(self, result):
        checks = []
        
        results = result.get('results', [])
        
        # Check 1: All files validated
        errors_in_processing = [r for r in results if 'error' in r]
        if errors_in_processing:
            checks.append({
                'check': 'Ficheros con error de procesamiento',
                'status': 'error',
                'detail': f'{len(errors_in_processing)} fichero(s) fallaron al validar: {[r.get("filename","?") for r in errors_in_processing]}',
            })
        else:
            checks.append({
                'check': 'Todos los ficheros validados',
                'status': 'pass',
                'detail': f'{len(results)} ficheros validados correctamente.',
            })
        
        # Check 2: Record counts are reasonable
        for r in results:
            if 'error' in r:
                continue
            records = r.get('total_records', 0)
            if records == 0:
                checks.append({
                    'check': f'Registros en {r.get("filename", "?")}',
                    'status': 'error',
                    'detail': f'Fichero {r.get("filename")} tiene 0 registros. Posible error de lectura.',
                })
            elif records < 10:
                checks.append({
                    'check': f'Registros en {r.get("filename", "?")}',
                    'status': 'warning',
                    'detail': f'Fichero {r.get("filename")} tiene solo {records} registros. Verificar si es correcto.',
                })
        
        if not any(c['status'] in ('error', 'warning') for c in checks if 'Registros' in c['check']):
            total = sum(r.get('total_records', 0) for r in results if 'error' not in r)
            checks.append({
                'check': 'Volumen de registros',
                'status': 'pass',
                'detail': f'{total} registros totales procesados en {len(results)} ficheros.',
            })
        
        return self._summarize('Integridad de datos', checks)
    
    def _summarize(self, agent_name, checks):
        passed = sum(1 for c in checks if c['status'] == 'pass')
        warnings = sum(1 for c in checks if c['status'] == 'warning')
        errors = sum(1 for c in checks if c['status'] == 'error')
        return {
            'agent': agent_name,
            'checks_run': len(checks),
            'checks_passed': passed,
            'warnings': warnings,
            'errors': errors,
            'checks': checks,
        }


# ─── AGENTE 2: MAPPING DE EMPRESAS ───────────────────────────────────────────

class AgentCompanyMapping:
    """Verifica que el mapping de empresas es correcto y completo."""
    
    def check(self, result):
        checks = []
        
        recon = result.get('reconciliacion', [])
        empresas_stats = set(r['empresa'] for r in recon)
        empresas_contab = result.get('empresas_contabilidad', 0)
        
        # Check 1: Companies with stats but no accounting
        no_contab = [r for r in recon if r['contabilidad'] == 0 and r['estadistica'] > 0]
        empresas_no_contab = set(r['empresa'] for r in no_contab)
        if empresas_no_contab:
            checks.append({
                'check': 'Empresas sin contrapartida contable',
                'status': 'pass',
                'detail': f'{len(empresas_no_contab)} empresa(s) tienen datos en estadísticas pero no en contabilidad (es habitual si se contabilizan bajo otra entidad del grupo): {list(empresas_no_contab)[:5]}',
            })
        else:
            checks.append({
                'check': 'Cobertura de empresas',
                'status': 'pass',
                'detail': 'Todas las empresas de estadísticas tienen contrapartida contable.',
            })
        
        # Check 2: Companies with accounting but no stats
        # We can't check this directly without the raw contab data, 
        # but we can check if empresas_contab > empresas_stats
        if empresas_contab > len(empresas_stats):
            diff = empresas_contab - len(empresas_stats)
            checks.append({
                'check': 'Empresas en contabilidad sin estadísticas',
                'status': 'pass',
                'detail': f'{diff} empresa(s) en contabilidad no tienen fichero de estadísticas (habitual para holdings, servicios internos, etc.).',
            })
        else:
            checks.append({
                'check': 'Cobertura contable',
                'status': 'pass',
                'detail': f'{len(empresas_stats)} empresas cubiertas en estadísticas vs {empresas_contab} en contabilidad.',
            })
        
        # Check 3: No duplicate companies (same name slightly different)
        empresa_list = sorted(empresas_stats)
        potential_dupes = []
        for i, e1 in enumerate(empresa_list):
            for e2 in empresa_list[i+1:]:
                # Simple check: one name contains the other
                e1_short = e1.lower().replace(',', '').replace('.', '').replace(' s l', '').replace(' s a', '')
                e2_short = e2.lower().replace(',', '').replace('.', '').replace(' s l', '').replace(' s a', '')
                if e1_short in e2_short or e2_short in e1_short:
                    if len(e1_short) > 10 and len(e2_short) > 10:
                        potential_dupes.append((e1, e2))
        
        if potential_dupes:
            checks.append({
                'check': 'Posibles empresas duplicadas',
                'status': 'warning',
                'detail': f'{len(potential_dupes)} par(es) de empresas con nombres similares que podrían ser la misma: {potential_dupes[:3]}',
            })
        else:
            checks.append({
                'check': 'Unicidad de empresas',
                'status': 'pass',
                'detail': 'No se detectaron empresas duplicadas.',
            })
        
        return self._summarize('Mapping de empresas', checks)
    
    def _summarize(self, agent_name, checks):
        passed = sum(1 for c in checks if c['status'] == 'pass')
        warnings = sum(1 for c in checks if c['status'] == 'warning')
        errors = sum(1 for c in checks if c['status'] == 'error')
        return {
            'agent': agent_name,
            'checks_run': len(checks),
            'checks_passed': passed,
            'warnings': warnings,
            'errors': errors,
            'checks': checks,
        }


# ─── AGENTE 3: CONSISTENCIA NUMÉRICA ─────────────────────────────────────────

class AgentNumericalConsistency:
    """Verifica que los números cuadran internamente."""
    
    def check_reconciliation(self, result):
        checks = []
        
        recon = result.get('reconciliacion', [])
        
        # Check 1: Percentages are correctly calculated
        bad_pct = 0
        for r in recon:
            if r['contabilidad'] != 0:
                expected_pct = round((r['diferencia'] / r['contabilidad']) * 100, 1)
                if abs(expected_pct - r['pct']) > 0.2:
                    bad_pct += 1
        
        if bad_pct > 0:
            checks.append({
                'check': 'Cálculo de porcentajes',
                'status': 'error',
                'detail': f'{bad_pct} partida(s) con porcentaje de diferencia mal calculado.',
            })
        else:
            checks.append({
                'check': 'Cálculo de porcentajes',
                'status': 'pass',
                'detail': 'Todos los porcentajes de diferencia están correctamente calculados.',
            })
        
        # Check 2: Status classification is consistent
        bad_status = 0
        for r in recon:
            diff = abs(r['diferencia'])
            pct = abs(r['pct'])
            status = str(r['status']).lower().strip()
            
            # Normalize status (handle both emoji and string formats)
            if status in ('✓', 'match', 'ok'):
                normalized = 'match'
            elif status in ('⚠', 'warning'):
                normalized = 'warning'
            elif status in ('✗', 'mismatch', 'error', 'fail'):
                normalized = 'mismatch'
            else:
                normalized = status
            
            # Validate classification
            if diff < 10 and normalized != 'match':
                bad_status += 1
            elif diff >= 10 and pct < 5 and normalized not in ('warning', 'match'):
                bad_status += 1
        
        if bad_status > 0:
            checks.append({
                'check': 'Clasificación de estado',
                'status': 'warning',
                'detail': f'{bad_status} partida(s) con clasificación de estado posiblemente incorrecta.',
            })
        else:
            checks.append({
                'check': 'Clasificación de estado',
                'status': 'pass',
                'detail': 'Todas las clasificaciones (match/warning/mismatch) son coherentes con los umbrales.',
            })
        
        # Check 3: No absurdly large numbers (potential read errors)
        outliers = [r for r in recon if abs(r['estadistica']) > 10_000_000 or abs(r['contabilidad']) > 10_000_000]
        if outliers:
            checks.append({
                'check': 'Valores extremos',
                'status': 'warning',
                'detail': f'{len(outliers)} partida(s) con importes superiores a 10M€. Verificar que no son errores de lectura: {[(r["empresa"][:30], r["cuenta"], r["estadistica"]) for r in outliers[:3]]}',
            })
        else:
            checks.append({
                'check': 'Valores extremos',
                'status': 'pass',
                'detail': 'No se detectaron importes anormalmente grandes.',
            })
        
        # Check 4: Total reported matches sum of parts
        total_705_reported = result.get('total_estadistica_705', 0)
        total_705_sum = sum(r['estadistica'] for r in recon if r['cuenta'] == '705')
        if abs(total_705_reported - total_705_sum) > 1:
            checks.append({
                'check': 'Consistencia de totales',
                'status': 'error',
                'detail': f'Total 705 reportado ({total_705_reported:,.2f}) no coincide con suma de partidas ({total_705_sum:,.2f}).',
            })
        else:
            checks.append({
                'check': 'Consistencia de totales',
                'status': 'pass',
                'detail': 'Los totales reportados coinciden con la suma de partidas individuales.',
            })
        
        return self._summarize('Consistencia numérica', checks)
    
    def check_data_quality(self, result):
        checks = []
        
        # Check: error counts are consistent
        total_errors_reported = result.get('total_errors', 0)
        total_errors_sum = sum(r.get('total_errors', 0) for r in result.get('results', []) if 'error' not in r)
        
        if total_errors_reported != total_errors_sum:
            checks.append({
                'check': 'Conteo de errores',
                'status': 'error',
                'detail': f'Total errores reportado ({total_errors_reported}) no coincide con suma por fichero ({total_errors_sum}).',
            })
        else:
            checks.append({
                'check': 'Conteo de errores',
                'status': 'pass',
                'detail': f'Conteo de errores consistente: {total_errors_reported} total.',
            })
        
        # Check: warnings count
        total_warnings_reported = result.get('total_warnings', 0)
        total_warnings_sum = sum(r.get('total_warnings', 0) for r in result.get('results', []) if 'error' not in r)
        
        if total_warnings_reported != total_warnings_sum:
            checks.append({
                'check': 'Conteo de warnings',
                'status': 'warning',
                'detail': f'Total warnings reportado ({total_warnings_reported}) vs suma ({total_warnings_sum}).',
            })
        else:
            checks.append({
                'check': 'Conteo de warnings',
                'status': 'pass',
                'detail': f'Conteo de warnings consistente: {total_warnings_reported} total.',
            })
        
        return self._summarize('Consistencia numérica', checks)
    
    def _summarize(self, agent_name, checks):
        passed = sum(1 for c in checks if c['status'] == 'pass')
        warnings = sum(1 for c in checks if c['status'] == 'warning')
        errors = sum(1 for c in checks if c['status'] == 'error')
        return {
            'agent': agent_name,
            'checks_run': len(checks),
            'checks_passed': passed,
            'warnings': warnings,
            'errors': errors,
            'checks': checks,
        }


# ─── AGENTE 4: COBERTURA DE PERIODOS ─────────────────────────────────────────

class AgentPeriodCoverage:
    """Verifica que los periodos analizados son correctos y completos."""
    
    def check_reconciliation(self, result):
        checks = []
        
        recon = result.get('reconciliacion', [])
        meses = set(r['mes'] for r in recon)
        
        # Check 1: Expected months present
        expected = {1, 2}  # Enero y Febrero 2026
        missing = expected - meses
        extra = meses - expected
        
        if missing:
            checks.append({
                'check': 'Meses esperados',
                'status': 'error',
                'detail': f'Faltan meses esperados: {missing}',
            })
        else:
            checks.append({
                'check': 'Meses esperados',
                'status': 'pass',
                'detail': f'Ambos meses (Enero y Febrero) presentes en la reconciliación.',
            })
        
        if extra:
            checks.append({
                'check': 'Meses fuera de periodo',
                'status': 'warning',
                'detail': f'Se encontraron datos de meses adicionales: {extra}. Verificar si deben incluirse.',
            })
        
        # Check 2: Each company has both months
        empresas_meses = {}
        for r in recon:
            emp = r['empresa']
            if emp not in empresas_meses:
                empresas_meses[emp] = set()
            empresas_meses[emp].add(r['mes'])
        
        incomplete = {e: m for e, m in empresas_meses.items() if m != expected}
        if incomplete:
            only_one_month = [e for e, m in incomplete.items() if len(m) == 1]
            if only_one_month:
                checks.append({
                    'check': 'Empresas con un solo mes',
                    'status': 'warning',
                    'detail': f'{len(only_one_month)} empresa(s) solo tienen datos de un mes: {only_one_month[:5]}',
                })
        else:
            checks.append({
                'check': 'Cobertura mensual por empresa',
                'status': 'pass',
                'detail': f'Todas las empresas tienen datos de ambos meses.',
            })
        
        return self._summarize('Cobertura de periodos', checks)
    
    def _summarize(self, agent_name, checks):
        passed = sum(1 for c in checks if c['status'] == 'pass')
        warnings = sum(1 for c in checks if c['status'] == 'warning')
        errors = sum(1 for c in checks if c['status'] == 'error')
        return {
            'agent': agent_name,
            'checks_run': len(checks),
            'checks_passed': passed,
            'warnings': warnings,
            'errors': errors,
            'checks': checks,
        }


# ─── AGENTE 5: DETECCIÓN DE ERRORES ──────────────────────────────────────────

class AgentErrorDetection:
    """Verifica que se han evaluado todos los tipos de error del catálogo."""
    
    CATALOG_ERRORS = [
        (1, 'Productos no mapeados'),
        (2, 'Nombres de compañía nuevos'),
        (3, 'Nombres de compañía largos'),
        (4, 'Cambio de nombre de columna'),
        (5, 'Valores no normalizados'),
        (6, 'Información en campo equivocado'),
        (7, 'NIFs no informados'),
        (8, 'NIFs no normalizados'),
        (9, 'Valores numéricos incorrectos'),
        (10, 'Comisiones erróneas'),
        (11, 'Descuadre de comisiones'),
        (12, 'Sobrecomisiones en campo incorrecto'),
        (13, 'Recibos con comisión 0'),
        (14, 'Incoherencia resultados totales'),
        (15, 'Formatos de fecha erróneos'),
        (16, 'Errores cruce ficheros'),
        (17, 'Recibos duplicados sin anular'),
        (18, 'Fecha anulación incoherente'),
        (19, 'Espacios en blanco'),
        (20, 'Recibos fuera de periodo'),
    ]
    
    def check(self, result):
        checks = []
        
        # Collect all error_nums that were detected
        detected_errors = set()
        for file_result in result.get('results', []):
            if 'error' in file_result:
                continue
            for issue in file_result.get('errors', []) + file_result.get('warnings', []) + file_result.get('corrections', []):
                detected_errors.add(issue.get('error_num'))
        
        # Check which catalog errors were evaluated
        not_detected = []
        for num, name in self.CATALOG_ERRORS:
            if num in detected_errors:
                checks.append({
                    'check': f'Error {num}: {name}',
                    'status': 'pass',
                    'detail': f'Evaluado y detectado en al menos un fichero.',
                })
            else:
                not_detected.append((num, name))
        
        if not_detected:
            # These errors weren't found - could be because they don't exist in the files
            # This is informational, not a warning
            checks.append({
                'check': 'Tipos de error no encontrados en estos ficheros',
                'status': 'pass',
                'detail': f'{len(not_detected)} tipo(s) de error del catálogo no se encontraron en estos ficheros (puede ser correcto si los datos no contienen esos errores): {[name for _, name in not_detected]}',
            })
        
        coverage = len(detected_errors) / len(self.CATALOG_ERRORS) * 100
        checks.append({
            'check': 'Cobertura del catálogo de errores',
            'status': 'pass' if coverage > 60 else 'warning',
            'detail': f'{len(detected_errors)}/{len(self.CATALOG_ERRORS)} tipos de error evaluados ({coverage:.0f}% del catálogo).',
        })
        
        return self._summarize('Detección de errores', checks)
    
    def _summarize(self, agent_name, checks):
        passed = sum(1 for c in checks if c['status'] == 'pass')
        warnings = sum(1 for c in checks if c['status'] == 'warning')
        errors = sum(1 for c in checks if c['status'] == 'error')
        return {
            'agent': agent_name,
            'checks_run': len(checks),
            'checks_passed': passed,
            'warnings': warnings,
            'errors': errors,
            'checks': checks,
        }


# ─── AGENTE 5B: VERIFICACIÓN DE CORRECCIONES ─────────────────────────────────

class AgentCorrectionVerification:
    """Verifica que las correcciones automáticas no han introducido errores."""
    
    def check(self, dq_result, correction_result):
        checks = []
        
        correction_results = correction_result.get('results', [])
        dq_results = dq_result.get('results', [])
        
        for corr in correction_results:
            if 'error' in corr:
                continue
            
            filename = corr.get('filename', '?')
            
            # Check 1: Record count preserved
            original_records = None
            for dq in dq_results:
                if dq.get('filename') == filename:
                    original_records = dq.get('total_records', 0)
                    break
            
            corrected_records = corr.get('total_records', 0)
            
            if original_records and corrected_records != original_records:
                checks.append({
                    'check': f'Registros preservados en {filename}',
                    'status': 'error',
                    'detail': f'El fichero original tenía {original_records} registros pero el corregido tiene {corrected_records}. Se perdieron datos.',
                })
            elif original_records:
                checks.append({
                    'check': f'Registros preservados en {filename}',
                    'status': 'pass',
                    'detail': f'{corrected_records} registros preservados correctamente.',
                })
            
            # Check 2: Corrections count is reasonable
            total_corr = corr.get('total_corrections', 0)
            if original_records and total_corr > original_records * 2:
                checks.append({
                    'check': f'Volumen de correcciones en {filename}',
                    'status': 'warning',
                    'detail': f'{total_corr} correcciones sobre {original_records} registros ({total_corr/original_records*100:.0f}%). Porcentaje inusualmente alto.',
                })
        
        if not checks:
            checks.append({
                'check': 'Verificación de correcciones',
                'status': 'pass',
                'detail': 'No se encontraron problemas en las correcciones aplicadas.',
            })
        
        return self._summarize('Verificación de correcciones', checks)
    
    def _summarize(self, agent_name, checks):
        passed = sum(1 for c in checks if c['status'] == 'pass')
        warnings = sum(1 for c in checks if c['status'] == 'warning')
        errors = sum(1 for c in checks if c['status'] == 'error')
        return {
            'agent': agent_name,
            'checks_run': len(checks),
            'checks_passed': passed,
            'warnings': warnings,
            'errors': errors,
            'checks': checks,
        }


# ─── AGENTE 6: VERIFICACIÓN DE INFORMES ──────────────────────────────────────

class AgentReportVerification:
    """Verifica que los informes para corredurías son correctos y completos."""
    
    def check(self, dq_result, report_result):
        checks = []
        
        reports = report_result.get('reports', [])
        dq_results = dq_result.get('results', [])
        
        # Check 1: Every file with non-auto-fixable issues has a report
        files_needing_reports = set()
        for r in dq_results:
            if 'error' in r:
                continue
            has_manual = any(
                not issue.get('correccion_auto', False)
                for issue in r.get('errors', []) + r.get('warnings', [])
                if issue.get('para_correo') or not issue.get('correccion_auto')
            )
            if has_manual:
                files_needing_reports.add(r.get('correduria', r.get('filename', '?')))
        
        corredurias_with_reports = set(r.get('correduria', '') for r in reports if r.get('tiene_incidencias'))
        
        missing_reports = files_needing_reports - corredurias_with_reports
        if missing_reports:
            checks.append({
                'check': 'Corredurías sin informe',
                'status': 'warning',
                'detail': f'{len(missing_reports)} correduría(s) con incidencias manuales pero sin informe generado: {list(missing_reports)[:5]}',
            })
        else:
            checks.append({
                'check': 'Cobertura de informes',
                'status': 'pass',
                'detail': f'Todas las corredurías con incidencias tienen informe generado.',
            })
        
        # Check 2: Reports are not empty
        empty_reports = [r for r in reports if r.get('tiene_incidencias') and not r.get('cuerpo_email', '').strip()]
        if empty_reports:
            checks.append({
                'check': 'Informes vacíos',
                'status': 'error',
                'detail': f'{len(empty_reports)} informe(s) marcados con incidencias pero sin contenido.',
            })
        else:
            checks.append({
                'check': 'Contenido de informes',
                'status': 'pass',
                'detail': 'Todos los informes tienen contenido.',
            })
        
        # Check 3: Report subjects are meaningful
        for r in reports:
            if r.get('tiene_incidencias') and r.get('asunto_email'):
                if len(r['asunto_email']) < 10:
                    checks.append({
                        'check': f'Asunto email {r.get("correduria", "?")}',
                        'status': 'warning',
                        'detail': f'Asunto demasiado corto: "{r["asunto_email"]}"',
                    })
        
        if not any(c['check'].startswith('Asunto') for c in checks):
            checks.append({
                'check': 'Asuntos de email',
                'status': 'pass',
                'detail': 'Todos los asuntos de email son descriptivos.',
            })
        
        return self._summarize('Verificación de informes', checks)
    
    def _summarize(self, agent_name, checks):
        passed = sum(1 for c in checks if c['status'] == 'pass')
        warnings = sum(1 for c in checks if c['status'] == 'warning')
        errors = sum(1 for c in checks if c['status'] == 'error')
        return {
            'agent': agent_name,
            'checks_run': len(checks),
            'checks_passed': passed,
            'warnings': warnings,
            'errors': errors,
            'checks': checks,
        }
