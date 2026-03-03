"""
Sistema Avançado de Auditoria de Condomínios com IA Completa
Integra múltiplos tipos de IA: ML, NLP, Predição e Análise Temporal
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
import os
import json
import re

from app.core import ConfigManager, SystemConfig, AuditLogger
from app.data_input_manager import DataInputManager, DataInputValidator
from app.extraction.legacy import categorize_transactions, check_extraction_quality
from app.analysis import AdvancedAIEngine, NLPAnalyzer, PredictiveAI
from app.reporting.report_generator import generate_full_report, generate_conference_report
from app.reporting.alert_generator import add_alerts_to_audit_result
from .audit_structures import make_warning, error_from_exception, WarningCode
from .labor_analyzer import analyze_labor_charges, is_folha_invalida, refine_base_calculo_from_holerites, refine_irrf_with_holerites
from app.extraction.llm import extract_labor_data_from_docs, merge_labor_with_llm, should_trigger_llm
from app.extraction.legacy.holerite_extractor import (
    extract_holerites_hybrid,
    extract_holerites_from_dataframe,
    deduplicate_holerites,
    collect_holerite_extraction_debug,
)
from .financial_consolidator import calculate_financial_totals_correct

class AdvancedAuditSystem:
    """Sistema avançado de auditoria com IA completa"""
    
    def __init__(self, config_file: Optional[str] = None):
        """Inicializa o sistema avançado de auditoria"""
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.config
        self.logger = AuditLogger(self.config)
        self.data_manager = DataInputManager(self.config, self.logger)
        self.validator = DataInputValidator()
        
        # Inicializar motores de IA
        self.ai_engine = AdvancedAIEngine()
        self.nlp_analyzer = NLPAnalyzer()
        self.predictive_ai = PredictiveAI()
        
        # Criar diretório de saída
        os.makedirs(self.config.output_directory, exist_ok=True)
        
        self.logger.info("Sistema Avançado de Auditoria com IA Completa inicializado")
    
    def run_comprehensive_audit(
        self,
        file_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        df_input: Optional[pd.DataFrame] = None,
        document_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executa auditoria completa com IA avançada
        
        Args:
            file_path: Caminho para o arquivo de dados (opcional se df_input fornecido)
            output_dir: Diretório de saída (opcional)
            df_input: DataFrame com dados já carregados em memória (evita leitura duplicada)
            document_context: Contexto opcional sobre documentos (total_files, by_category, etc.)
                             para geração de alertas (documentos principais incompletos, etc.)
            
        Returns:
            Dict com resultados completos da auditoria
        """
        audit_results = {
            'success': False,
            'file_path': file_path or 'in_memory',
            'start_time': datetime.now(),
            'end_time': None,
            'total_transactions': 0,
            'anomalies_detected': 0,
            'ai_analysis': {},
            'nlp_analysis': {},
            'predictive_analysis': {},
            'report_file': None,
            'errors': [],
            'warnings': [],
            'alerts': [],
            'summary': {},
            'document_context': document_context or {},
        }
        
        try:
            if file_path:
                self.logger.log_audit_start(file_path)
            else:
                self.logger.info("Iniciando auditoria com dados em memória")
            
            # Fase 1: Carregamento e validação de dados
            self.logger.info("Fase 1: Carregamento e validacao de dados")
            load_metadata = {}  # nome do condomínio extraído do PDF bruto (quando carrega por file_path)
            if df_input is not None:
                # Usar DataFrame já carregado em memória (evita leitura duplicada)
                validation_warnings = self._validate_dataframe(df_input, document_context=audit_results.get("document_context") or {})
                df = df_input  # DataFrame já validado
                # Adicionar warnings ao audit_results
                audit_results['warnings'].extend(validation_warnings)
            elif file_path is not None:
                # Carregar do arquivo; metadata guarda nome do condomínio extraído do PDF bruto
                df, load_metadata = self._load_and_validate_data(file_path)
                if load_metadata.get("condominio_name") and not (audit_results.get("document_context") or {}).get("condominio_name"):
                    audit_results.setdefault("document_context", {})["condominio_name"] = load_metadata["condominio_name"]
            else:
                raise ValueError("É necessário fornecer 'file_path' ou 'df_input'")
            audit_results['total_transactions'] = len(df)

            # Modo estrutural (LLM retornou apenas contas/totais): permitir gerar relatório sem transações
            if df is not None and len(df) == 0:
                doc_ctx = audit_results.get("document_context") or {}
                totals = doc_ctx.get("totals_extracted") or {}
                values = totals.get("values", totals) if isinstance(totals, dict) else {}
                has_structural = bool(doc_ctx.get("structural_extraction")) or bool(doc_ctx.get("structural_extraction_periods"))
                has_saldo_final = isinstance(values, dict) and values.get("saldo_final") is not None
                if has_structural or has_saldo_final:
                    self.logger.warning("DataFrame vazio; gerando relatório em modo estrutural (contas/totais).")
                    audit_results["summary"] = {
                        "mode": "structural_only",
                        "has_structural_extraction": bool(doc_ctx.get("structural_extraction")),
                        "has_totals_extracted": bool(values),
                    }
                    # Fase 7: Geração de relatório
                    self.logger.info("Fase 7: Geracao de relatorio avancado")
                    report_file = self._generate_advanced_report(df, audit_results, output_dir)
                    audit_results["report_file"] = report_file
                    audit_results["success"] = True
                    return audit_results
            
            # Fase 2: Processamento básico
            self.logger.info("Fase 2: Processamento e categorizacao")
            df_processed, process_metadata = self._process_data(df)
            # Nome do condomínio (document_context/LLM ou metadata do load) — repassar ao relatório
            if process_metadata.get("condominio_name") and not (audit_results.get("document_context") or {}).get("condominio_name"):
                audit_results.setdefault("document_context", {})["condominio_name"] = process_metadata["condominio_name"]
            # Verificação de qualidade da extração (nome do condomínio: do bruto no load ou no process)
            condominio_from_raw = process_metadata.get("condominio_name") or load_metadata.get("condominio_name")
            quality = check_extraction_quality(
                df_processed,
                source_hint=audit_results.get("file_path", ""),
                condominio_name_from_raw=condominio_from_raw,
                period_start=process_metadata.get("period_start"),
                period_end=process_metadata.get("period_end"),
                ocr_used=process_metadata.get("ocr_used"),
                ocr_text_len=process_metadata.get("ocr_text_len"),
            )
            audit_results["extraction_quality"] = quality
            # Repassar qualidade de extração no document_context para regras de alertas
            audit_results.setdefault("document_context", {})["extraction_quality"] = quality

            # Fase 3: Análise com IA Avançada
            self.logger.info("Fase 3: Analise com IA Avancada")
            df_ai_analyzed = self._run_advanced_ai_analysis(df_processed)
            audit_results['ai_analysis'] = self._extract_ai_insights(df_ai_analyzed)
            
            # Fase 4: Análise NLP
            self.logger.info("Fase 4: Analise de Linguagem Natural")
            df_nlp_analyzed = self._run_nlp_analysis(df_ai_analyzed)
            audit_results['nlp_analysis'] = self._extract_nlp_insights(df_nlp_analyzed)
            
            # Fase 5: IA Preditiva
            self.logger.info("Fase 5: IA Preditiva e Analise de Riscos")
            predictive_results = self._run_predictive_analysis(df_nlp_analyzed)
            audit_results['predictive_analysis'] = predictive_results
            
            # Fase 6: Consolidação de resultados
            self.logger.info("Fase 6: Consolidacao de resultados")
            df_final = self._consolidate_results(df_nlp_analyzed, audit_results)
            audit_results['anomalies_detected'] = len(df_final[df_final['anomalia_detectada'] == True])

            # Analise de encargos trabalhistas (base); contexto pode trazer base_folha_mes_anterior (X-1)
            doc_ctx = audit_results.get("document_context") or {}
            labor_analysis = analyze_labor_charges(df_final, document_context=doc_ctx)
            audit_results["labor_analysis"] = labor_analysis

            # Extração estruturada de holerites (sempre quando detectado)
            doc_texts = (audit_results.get("document_context") or {}).get("document_texts", [])
            holerites_extraidos = []
            # Holerites extraídos de links FGTS/holerite (quando FETCH_FGTS_LINKS=1)
            holerites_from_fgts = (audit_results.get("document_context") or {}).get("holerites_from_fgts_links") or []
            if holerites_from_fgts:
                holerites_extraidos.extend(holerites_from_fgts)
                self.logger.info(
                    f"Incluídos {len(holerites_from_fgts)} holerite(s) extraídos de links FGTS/holerite"
                )

            # PRIORIDADE 1: Extração direta do DataFrame (sempre tentar quando houver planilhas)
            original_dataframes = (audit_results.get("document_context") or {}).get("original_dataframes", [])
            if original_dataframes:
                for df_info in original_dataframes:
                    if isinstance(df_info, dict):
                        df_orig = df_info.get("dataframe")
                        filename = df_info.get("filename", "")
                        file_ext = os.path.splitext(filename)[1].lower() if filename else ""
                        
                        # Para ODS/Excel, tentar extração direta do DataFrame primeiro
                        if file_ext in ['.ods', '.sxc', '.xlsx', '.xls', '.xlt'] and df_orig is not None and not df_orig.empty:
                            try:
                                df_holerites = extract_holerites_from_dataframe(df_orig, filename)
                                if df_holerites:
                                    holerites_extraidos.extend(df_holerites)
                                    self.logger.info(f"Extraídos {len(df_holerites)} holerite(s) diretamente do DataFrame {filename}")
                            except Exception as e:
                                self.logger.warning(f"Erro ao extrair holerites do DataFrame {filename}: {e}")
            
            # PRIORIDADE 2: Extração via regex em texto (sempre tentar quando houver documento texto)
            if doc_texts:
                try:
                    # LLM só é usado depois se não encontramos holerites suficientes via DataFrame/regex
                    regex_holerites = extract_holerites_hybrid(doc_texts, force_llm=False)
                    if regex_holerites:
                        # Combinar com holerites já extraídos (evitar duplicatas)
                        seen_keys = set((h.get("funcionario", ""), h.get("periodo", ""), h.get("salario_bruto", 0)) for h in holerites_extraidos)
                        for h in regex_holerites:
                            key = (h.get("funcionario", ""), h.get("periodo", ""), h.get("salario_bruto", 0))
                            if key not in seen_keys:
                                holerites_extraidos.append(h)
                                seen_keys.add(key)
                        self.logger.info(f"Adicionados {len(regex_holerites)} holerite(s) via regex em texto")
                except Exception as e:
                    self.logger.warning(f"Erro ao extrair holerites via regex: {e}")

            # Deduplicar holerites 100% idênticos da mesma origem (source_file)
            if holerites_extraidos:
                try:
                    deduped, n_dup = deduplicate_holerites(holerites_extraidos)
                    if deduped or not holerites_extraidos:
                        holerites_extraidos = deduped
                        if n_dup:
                            self.logger.info(f"Removidos {n_dup} holerite(s) duplicado(s) da mesma origem")
                    else:
                        self.logger.warning("deduplicate_holerites retornou lista vazia de input não-vazio; mantendo original")
                except Exception as _e:
                    self.logger.warning(f"Erro em deduplicate_holerites: {_e}; mantendo original")

            # Integrar holerites extraídos no labor_analysis
            if holerites_extraidos:
                self.logger.info(f"Total de {len(holerites_extraidos)} holerite(s) estruturado(s) extraído(s)")
                if "base_calculo" not in labor_analysis:
                    labor_analysis["base_calculo"] = {}
                labor_analysis["base_calculo"]["holerites_detalhados"] = holerites_extraidos
                # REGRA 5: Holerite inválido invalida análise trabalhista.
                if is_folha_invalida(holerites_extraidos):
                    labor_analysis["folha_invalida"] = True
                # Base de cálculo: quando a prestação não trouxer folha, usar soma dos brutos dos holerites
                refine_base_calculo_from_holerites(labor_analysis)
                # IRRF: só é possível validar com holerites individuais (recolhido por salário)
                refine_irrf_with_holerites(labor_analysis, holerites_extraidos)
                audit_results["labor_analysis"] = labor_analysis
            elif doc_texts:
                self.logger.warning(
                    f"[ALERTA] Nenhum holerite estruturado extraído dos documentos de texto "
                    f"(qtd_docs={len(doc_texts)}). Verifique logs de services.holerite_extractor "
                    f"para diagnóstico por arquivo."
                )

            audit_results["holerites_extraidos"] = holerites_extraidos
            # Diagnóstico para frontend/API: por que cada documento extraiu (ou não) holerites.
            audit_results["holerite_extraction_debug"] = collect_holerite_extraction_debug(doc_texts)

            # LLM fallback para holerites/encargos quando dados estiverem ausentes
            # Só tentar LLM se já temos holerites extraídos via regex OU se temos API key configurada
            api_key_available = bool(os.getenv("OPENAI_API_KEY"))
            if should_trigger_llm(labor_analysis) and doc_texts:
                if api_key_available:
                    llm_result = extract_labor_data_from_docs(doc_texts)
                    audit_results["llm_extractions"] = llm_result
                    if llm_result.get("enabled"):
                        audit_results["labor_analysis"] = merge_labor_with_llm(labor_analysis, llm_result)
                        # Se LLM extraiu holerites e não tínhamos antes, adicionar
                        llm_holerites = llm_result.get("holerites", [])
                        if llm_holerites and not holerites_extraidos:
                            audit_results["holerites_extraidos"] = llm_holerites
                            if "base_calculo" not in audit_results["labor_analysis"]:
                                audit_results["labor_analysis"]["base_calculo"] = {}
                            audit_results["labor_analysis"]["base_calculo"]["holerites_detalhados"] = llm_holerites
                            refine_base_calculo_from_holerites(audit_results["labor_analysis"])
                            refine_irrf_with_holerites(audit_results["labor_analysis"], llm_holerites)
                else:
                    # Sem API key, apenas registrar que não foi usado (não é erro)
                    audit_results["llm_extractions"] = {
                        "enabled": False, 
                        "reason": "missing_api_key",
                        "note": "LLM não disponível (OPENAI_API_KEY não configurada). Extração via regex foi utilizada."
                    }
            elif doc_texts:
                audit_results["llm_extractions"] = {"enabled": False, "reason": "not_needed"}

            # Quando a extração financeira foi via LLM, incluir holerites e encargos no llm_extractions para o relatório
            doc_ctx = audit_results.get("document_context") or {}
            if doc_ctx.get("llm_extraction"):
                existing = audit_results.get("llm_extractions")
                if not isinstance(existing, dict):
                    existing = {}
                holerites_llm = doc_ctx.get("holerites_from_llm")
                if holerites_llm and isinstance(holerites_llm, list):
                    base = list(existing.get("holerites") or [])
                    seen = set((str((h.get("funcionario") or "")), str((h.get("periodo") or "")), (h.get("salario_bruto") or 0)) for h in base if isinstance(h, dict))
                    for h in holerites_llm:
                        if not isinstance(h, dict):
                            continue
                        key = (str(h.get("funcionario") or ""), str(h.get("periodo") or ""), (h.get("salario_bruto") or 0))
                        if key not in seen:
                            base.append(h)
                            seen.add(key)
                    existing["holerites"] = base
                encargos_llm = doc_ctx.get("encargos_from_llm")
                if encargos_llm and isinstance(encargos_llm, dict):
                    existing["encargos"] = {**(existing.get("encargos") or {}), **encargos_llm}
                if holerites_llm or encargos_llm:
                    existing["enabled"] = True
                    existing["reason"] = existing.get("reason") or "financial_llm"
                audit_results["llm_extractions"] = existing

            # Blindagem: após qualquer merge/fallback, manter holerites sincronizados no resultado final.
            # Isso evita perder holerites extraídos em etapas intermediárias antes da geração do relatório.
            final_holerites = audit_results.get("holerites_extraidos") or holerites_extraidos or []
            if final_holerites and isinstance(audit_results.get("labor_analysis"), dict):
                labor_final = audit_results["labor_analysis"]
                base_final = labor_final.get("base_calculo")
                if not isinstance(base_final, dict):
                    labor_final["base_calculo"] = {}
                    base_final = labor_final["base_calculo"]
                if not base_final.get("holerites_detalhados"):
                    base_final["holerites_detalhados"] = final_holerites
                    self.logger.info(
                        "Blindagem de holerite aplicada: holerites_extraidos sincronizados em labor_analysis.base_calculo"
                    )
                audit_results["holerites_extraidos"] = final_holerites
            
            # Resumo final e alertas (antes do relatório, para o modelo Resultado da Conferência)
            audit_results['summary'] = self._generate_comprehensive_summary(df_final, audit_results)
            add_alerts_to_audit_result(
                audit_results,
                df=df_final,
                document_context=audit_results.get("document_context"),
            )
            # Falhas de extração: "não consegui ler (conteúdo) na (linha) (página)" para análise manual
            extraction_failures = []
            eq = audit_results.get("extraction_quality") or {}
            if eq.get("details", {}).get("valores_zerados"):
                # Tentar encontrar página/linha de valores zerados no DataFrame
                page_info = None
                line_info = None
                if df_final is not None and not df_final.empty:
                    # Procurar por linhas com valores zerados que deveriam ter valor
                    zero_val_rows = df_final[(df_final.get("valor", 0) == 0) & (df_final.get("descricao", "").astype(str).str.len() > 10)]
                    if not zero_val_rows.empty:
                        first_row = zero_val_rows.iloc[0]
                        if "_page" in first_row and pd.notna(first_row.get("_page")):
                            page_info = int(first_row["_page"])
                        if "_line" in first_row and pd.notna(first_row.get("_line")):
                            line_info = int(first_row["_line"])
                extraction_failures.append({
                    "content": "valores financeiros do documento",
                    "message": "Soma dos valores extraídos é zero. Recomenda-se análise manual.",
                    "page": page_info,
                    "line": line_info
                })
            for err in eq.get("errors") or []:
                extraction_failures.append({
                    "content": err if isinstance(err, str) else err.get("message", "erro"),
                    "message": str(err)
                })
            labor = audit_results.get("labor_analysis") or {}
            # Procurar linhas específicas onde encargos deveriam estar mas não foram extraídos
            if df_final is not None and not df_final.empty:
                desc_lower = df_final["descricao"].astype(str).str.lower()
                for enc_name, enc_data in (labor.get("encargos") or {}).items():
                    if isinstance(enc_data, dict) and enc_data.get("valor_pago", 0) == 0 and enc_data.get("status") == "nao_identificado":
                        # Procurar linha onde o encargo aparece mas valor é zero
                        enc_keywords = {"fgts": ["fgts"], "inss": ["inss"], "irrf": ["irrf", "ir rf"]}
                        keywords = enc_keywords.get(enc_name.lower(), [enc_name.lower()])
                        matching_rows = df_final[desc_lower.str.contains("|".join(keywords), case=False, na=False) & (df_final.get("valor", 0) == 0)]
                        page_info = None
                        line_info = None
                        if not matching_rows.empty:
                            first_match = matching_rows.iloc[0]
                            if "_page" in first_match and pd.notna(first_match.get("_page")):
                                page_info = int(first_match["_page"])
                            if "_line" in first_match and pd.notna(first_match.get("_line")):
                                line_info = int(first_match["_line"])
                        extraction_failures.append({
                            "content": f"valor {enc_name.upper()} na folha/planilha",
                            "message": f"Encargo {enc_name.upper()} não foi extraído com valor. Recomenda-se análise manual.",
                            "page": page_info,
                            "line": line_info
                        })
                for trib_name, trib_data in (labor.get("tributos") or {}).items():
                    if isinstance(trib_data, dict) and trib_data.get("valor_pago", 0) == 0 and trib_data.get("status") == "nao_identificado":
                        keywords = [trib_name.lower()]
                        matching_rows = df_final[desc_lower.str.contains("|".join(keywords), case=False, na=False) & (df_final.get("valor", 0) == 0)]
                        page_info = None
                        line_info = None
                        if not matching_rows.empty:
                            first_match = matching_rows.iloc[0]
                            if "_page" in first_match and pd.notna(first_match.get("_page")):
                                page_info = int(first_match["_page"])
                            if "_line" in first_match and pd.notna(first_match.get("_line")):
                                line_info = int(first_match["_line"])
                        extraction_failures.append({
                            "content": f"valor {trib_name.upper()} na planilha",
                            "message": f"Tributo {trib_name.upper()} não foi extraído. Recomenda-se análise manual.",
                            "page": page_info,
                            "line": line_info
                        })
            else:
                # Fallback sem DataFrame: adicionar sem página/linha
                for enc_name, enc_data in (labor.get("encargos") or {}).items():
                    if isinstance(enc_data, dict) and enc_data.get("valor_pago", 0) == 0 and enc_data.get("status") == "nao_identificado":
                        extraction_failures.append({
                            "content": f"valor {enc_name.upper()} na folha/planilha",
                            "message": f"Encargo {enc_name.upper()} não foi extraído com valor. Recomenda-se análise manual."
                        })
                for trib_name, trib_data in (labor.get("tributos") or {}).items():
                    if isinstance(trib_data, dict) and trib_data.get("valor_pago", 0) == 0 and trib_data.get("status") == "nao_identificado":
                        extraction_failures.append({
                            "content": f"valor {trib_name.upper()} na planilha",
                            "message": f"Tributo {trib_name.upper()} não foi extraído. Recomenda-se análise manual."
                        })
            if extraction_failures:
                audit_results["extraction_failures"] = extraction_failures
            # Fase 7: Geração de relatório (formato Resultado da Conferência + detalhes técnicos)
            self.logger.info("Fase 7: Geracao de relatorio avancado")
            report_file = self._generate_advanced_report(df_final, audit_results, output_dir)
            audit_results['report_file'] = report_file
            audit_results['success'] = True
            
            self.logger.log_audit_end(
                audit_results['total_transactions'],
                audit_results['anomalies_detected']
            )
            
        except Exception as e:
            self.logger.log_error("auditoria avançada", e)
            audit_results['errors'].append(error_from_exception(e))
            
        finally:
            audit_results['end_time'] = datetime.now()
            audit_results['duration'] = audit_results['end_time'] - audit_results['start_time']
        
        return audit_results
    
    def _load_and_validate_data(self, file_path: str) -> tuple:
        """
        Carrega dados via DataInputManager (extração 100% LLM).
        Retorna (df, metadata) com metadata preenchido pelo load_data (ex.: condominio_name).
        """
        metadata = {}
        df = self.data_manager.load_data(file_path, metadata=metadata)
        return df, metadata
    
    def _validate_dataframe(self, df: pd.DataFrame, *, document_context: Optional[Dict[str, Any]] = None) -> list:
        """
        Valida DataFrame já carregado em memória
        
        Returns:
            Lista de warnings estruturados (dict com code, message, details, timestamp, severity)
        """
        # Permitir modo estrutural (contas/totais) sem transações
        if df is None or len(df) == 0:
            doc_ctx = document_context or {}
            totals = doc_ctx.get("totals_extracted") or {}
            values = totals.get("values", totals) if isinstance(totals, dict) else {}
            has_structural = bool(doc_ctx.get("structural_extraction")) or bool(doc_ctx.get("structural_extraction_periods"))
            has_saldo_final = isinstance(values, dict) and values.get("saldo_final") is not None
            if has_structural or has_saldo_final:
                warning_msg = "Validação: DataFrame vazio (modo estrutural com contas/totais da LLM)."
                self.logger.warning(warning_msg)
                return [
                    make_warning(
                        WarningCode.VALIDATION_WARNING,
                        warning_msg,
                        details={"context": "validation", "mode": "structural_only"},
                    )
                ]

        validation_errors = self.validator.validate_transaction_data(df)
        
        if validation_errors['critical']:
            raise ValueError(f"Erros críticos: {'; '.join(validation_errors['critical'])}")
        
        warnings_list = []
        for warning in validation_errors.get('warnings', []):
            warning_msg = f"Validação: {warning}"
            self.logger.warning(warning_msg)
            warnings_list.append(
                make_warning(
                    WarningCode.VALIDATION_WARNING,
                    warning_msg,
                    details={"raw": warning, "context": "validation"},
                )
            )
        return warnings_list
    
    def _process_data(self, df):
        """Categoriza os dados. O df já vem normalizado pela LLM (colunas data, descricao, tipo, valor)."""
        metadata = {}
        df_categorized = categorize_transactions(df)
        return df_categorized, metadata
    
    def _run_advanced_ai_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executa análise com IA avançada"""
        print("   Executando ensemble de modelos de ML...")
        df_ai = self.ai_engine.detect_anomalies(df)
        
        # Adicionar scores de confiança
        df_ai['ai_confidence_score'] = df_ai.apply(
            lambda row: min(1.0, row.get('ai_anomaly_score', 0) + 
                          row.get('iso_forest_score', 0) * 0.3 + 
                          row.get('svm_score', 0) * 0.3),
            axis=1
        )
        
        return df_ai
    
    def _run_nlp_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executa análise NLP"""
        print("   Analisando descricoes com NLP...")
        df_nlp = self.nlp_analyzer.analyze_descriptions(df)
        
        # Combinar scores NLP com IA
        df_nlp['combined_suspicion_score'] = df_nlp.apply(
            lambda row: min(1.0, 
                          row.get('ai_anomaly_score', 0) * 0.6 + 
                          row.get('nlp_suspicion_score', 0) * 0.4),
            axis=1
        )
        
        return df_nlp
    
    def _run_predictive_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Executa análise preditiva"""
        print("   Executando IA preditiva...")
        
        try:
            # Treinar modelos preditivos
            training_results = self.predictive_ai.train_predictive_models(df)
            
            if 'error' not in training_results:
                # Fazer predições
                predictions = self.predictive_ai.predict_future_risks(df)
                return predictions
            else:
                return {'error': training_results['error']}
                
        except Exception as e:
            self.logger.warning(f"Erro na análise preditiva: {str(e)}")
            return {'error': str(e)}
    
    def _consolidate_results(self, df: pd.DataFrame, audit_results: Dict) -> pd.DataFrame:
        """Consolida todos os resultados"""
        df_final = df.copy()
        
        # Atualizar coluna principal de anomalia
        df_final['anomalia_detectada'] = df_final.apply(
            lambda row: (
                row.get('ai_anomaly_detected', False) or 
                row.get('nlp_suspicion_score', 0) > 0.5 or
                row.get('combined_suspicion_score', 0) > 0.6
            ),
            axis=1
        )
        
        # Consolidar justificativas
        df_final['justificativa_anomalia'] = df_final.apply(
            lambda row: self._consolidate_justifications(row),
            axis=1
        )
        
        # Adicionar nível de risco
        df_final['nivel_risco'] = df_final.apply(
            lambda row: self._calculate_risk_level(row),
            axis=1
        )
        
        return df_final
    
    def _consolidate_justifications(self, row: pd.Series) -> str:
        """Consolida justificativas de diferentes análises"""
        justifications = []
        
        # Justificativas de IA
        if row.get('ai_justification'):
            justifications.append(f"IA: {row['ai_justification']}")
        
        # Justificativas NLP
        nlp_patterns = row.get('suspicious_patterns', [])
        if nlp_patterns:
            justifications.append(f"NLP: Padrões suspeitos: {', '.join(nlp_patterns)}")
        
        fraud_indicators = row.get('fraud_indicators', [])
        if fraud_indicators:
            justifications.append(f"NLP: Indicadores de fraude: {', '.join(fraud_indicators)}")
        
        # Justificativas de consistência
        consistency_score = row.get('consistency_score', 1.0)
        if consistency_score is not None and consistency_score < 0.7:
            justifications.append(f"Consistência: Score baixo ({consistency_score:.2f})")
        
        return "; ".join(justifications) if justifications else "Análise automática não detectou anomalias específicas"
    
    def _calculate_risk_level(self, row: pd.Series) -> str:
        """Calcula nível de risco consolidado"""
        combined_score = row.get('combined_suspicion_score', 0)
        
        # Garantir que combined_score não seja None
        if combined_score is None:
            combined_score = 0
        
        if combined_score > 0.8:
            return 'ALTO'
        elif combined_score > 0.6:
            return 'MÉDIO'
        elif combined_score > 0.3:
            return 'BAIXO'
        else:
            return 'MUITO BAIXO'
    
    def _extract_ai_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extrai insights da análise de IA (valores como int/float para evitar 'truth value of array ambiguous')."""
        if 'ai_confidence_score' in df.columns:
            ai_confidence_scores = df['ai_confidence_score']
            high_confidence_mask = ai_confidence_scores.notna() & (ai_confidence_scores > 0.8)
            high_confidence_count = int(high_confidence_mask.sum())
        else:
            high_confidence_count = 0
        
        insights = {
            'total_anomalies': int(df['ai_anomaly_detected'].sum()),
            'high_confidence_anomalies': high_confidence_count,
            'model_agreement': {},
            'feature_importance': self.ai_engine.get_feature_importance()
        }
        
        if 'iso_forest_score' in df.columns and 'svm_score' in df.columns:
            iso_anomalies = (df['iso_forest_score'] == 1)
            svm_anomalies = (df['svm_score'] == 1)
            agreement = int((iso_anomalies & svm_anomalies).sum())
            den = max(1, int(iso_anomalies.sum()) + int(svm_anomalies.sum()))
            insights['model_agreement'] = {
                'both_models': agreement,
                'agreement_rate': float(agreement) / den
            }
        
        return insights
    
    def _extract_nlp_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extrai insights da análise NLP"""
        nlp_report = self.nlp_analyzer.generate_nlp_report(df)
        
        return {
            'suspicious_patterns': nlp_report['suspicious_patterns'],
            'fraud_indicators': nlp_report['fraud_indicators'],
            'recommendations': nlp_report['recommendations'],
            'high_suspicion_count': nlp_report['summary']['high_suspicion_count']
        }
    
    def _generate_advanced_report(self, df: pd.DataFrame, audit_results: Dict, output_dir: Optional[str]) -> str:
        """Gera relatório no formato Resultado da Conferência + detalhes técnicos opcionais."""
        # Nome do condomínio: document_context (script) ou extraction_quality.details (LLM/metadata)
        doc_ctx = audit_results.get("document_context") or {}
        ext_details = (audit_results.get("extraction_quality") or {}).get("details") or {}
        condominio_name = (
            doc_ctx.get("condominio_name", "").strip()
            or ext_details.get("nome_condominio_extraido")
            or ""
        )
        if isinstance(condominio_name, str):
            condominio_name = condominio_name.strip()
        else:
            condominio_name = ""
        periodo_inicio = doc_ctx.get("periodo_inicio") or ""
        periodo_fim = doc_ctx.get("periodo_fim") or ""
        # Relatório principal no modelo "Resultado da Conferência"
        try:
            conference_report = generate_conference_report(
                df, audit_results,
                condominio_name=condominio_name,
                periodo_inicio=periodo_inicio or None,
                periodo_fim=periodo_fim or None,
            )
        except Exception as e:
            self.logger.error(f"Erro em generate_conference_report: {e}")
            raise
        try:
            basic_report = generate_full_report(df)
        except Exception as e:
            self.logger.error(f"Erro em generate_full_report: {e}")
            raise
        try:
            advanced_sections = self._generate_advanced_report_sections(audit_results)
        except Exception as e:
            self.logger.error(f"Erro em _generate_advanced_report_sections: {e}")
            raise
        full_report = conference_report + "\n\n---\n\n## Detalhes técnicos da análise\n\n" + basic_report + "\n\n" + advanced_sections
        
        # Salvar relatório
        if output_dir is None:
            output_dir = self.config.output_directory
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"relatorio_auditoria_avancada_{timestamp}.md"
        report_path = os.path.join(output_dir, report_filename)
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(full_report)
        
        self.logger.info(f"Relatório avançado salvo em: {report_path}")
        return report_path
    
    def _generate_advanced_report_sections(self, audit_results: Dict) -> str:
        """Gera seções avançadas do relatório"""
        sections = []
        
        # Seção de IA
        sections.append("# Análise de Inteligência Artificial\n")
        
        ai_analysis = audit_results.get('ai_analysis', {})
        sections.append(f"- **Anomalias detectadas por IA:** {ai_analysis.get('total_anomalies', 0)}")
        sections.append(f"- **Anomalias de alta confiança:** {ai_analysis.get('high_confidence_anomalies', 0)}")
        
        model_agreement = ai_analysis.get('model_agreement')
        if model_agreement is not None and isinstance(model_agreement, dict) and len(model_agreement) > 0:
            agreement = model_agreement
            sections.append(f"- **Concordância entre modelos:** {agreement.get('agreement_rate', 0):.2%}")
        
        # Seção NLP
        sections.append("\n# Análise de Linguagem Natural\n")
        
        nlp_analysis = audit_results.get('nlp_analysis', {})
        sections.append(f"- **Transações de alta suspeição:** {nlp_analysis.get('high_suspicion_count', 0)}")
        
        suspicious = nlp_analysis.get('suspicious_patterns')
        if suspicious is not None and isinstance(suspicious, dict) and len(suspicious) > 0:
            sections.append("- **Padrões suspeitos mais comuns:**")
            for pattern, count in list(suspicious.items())[:5]:
                sections.append(f"  - {pattern}: {count} ocorrências")
        
        # Seção Preditiva
        sections.append("\n# Análise Preditiva\n")
        
        predictive_analysis = audit_results.get('predictive_analysis', {})
        if 'error' not in predictive_analysis:
            risk_assessment = predictive_analysis.get('risk_assessment', {})
            sections.append(f"- **Nível de risco geral:** {risk_assessment.get('overall_risk_level', 'N/A')}")
            sections.append(f"- **Transações de alto risco:** {len(risk_assessment.get('high_risk_transactions', []))}")
            
            recommendations = predictive_analysis.get('recommendations', [])
            if recommendations:
                sections.append("- **Recomendações preditivas:**")
                for rec in recommendations[:5]:
                    sections.append(f"  - {rec}")
        else:
            sections.append(f"- **Erro na análise preditiva:** {predictive_analysis['error']}")
        
        # Seção de Insights
        sections.append("\n# Insights e Recomendações Gerais\n")
        
        total_anomalies = int(audit_results.get('anomalies_detected', 0))
        total_transactions = int(audit_results.get('total_transactions', 0))
        
        if total_transactions > 0:
            anomaly_rate = float(total_anomalies) / float(total_transactions)
            
            if anomaly_rate > 0.1:
                sections.append("🚨 **ALERTA:** Taxa de anomalias alta (>10%) - investigação prioritária recomendada")
            elif anomaly_rate > 0.05:
                sections.append("⚠️ **ATENÇÃO:** Taxa de anomalias moderada (>5%) - monitoramento intensificado recomendado")
            else:
                sections.append("✅ **OK:** Taxa de anomalias baixa (<5%) - situação normal")
        
        sections.append(f"- **Tempo total de análise:** {audit_results.get('duration', 'N/A')}")
        sections.append(f"- **Transações analisadas:** {total_transactions}")
        sections.append(f"- **Anomalias detectadas:** {total_anomalies}")
        
        return "\n".join(sections)
    
    def _generate_comprehensive_summary(self, df: pd.DataFrame, audit_results: Dict) -> Dict[str, Any]:
        """Gera resumo abrangente usando cálculo financeiro correto"""
        doc_ctx = audit_results.get("document_context") or {}
        saldo_inicial = doc_ctx.get("saldo_anterior")
        if saldo_inicial is not None and not isinstance(saldo_inicial, (int, float)):
            try:
                saldo_inicial = float(saldo_inicial)
            except (TypeError, ValueError):
                saldo_inicial = None
        financial_totals = calculate_financial_totals_correct(df, saldo_inicial=saldo_inicial)
        total_receitas = financial_totals["total_receitas"]
        total_despesas = financial_totals["total_despesas"]
        saldo = financial_totals["saldo"]
        
        # Análise por nível de risco (com 1 linha to_dict() pode gerar arrays -> evitar)
        risk_dict = {}
        if len(df) > 1:
            try:
                risk_analysis = df.groupby('nivel_risco').agg({
                    'valor': ['count', 'sum'],
                    'anomalia_detectada': 'sum'
                }).round(2)
                if not risk_analysis.empty:
                    risk_dict = risk_analysis.to_dict()
            except Exception:
                pass

        summary = {
            'financial_summary': {
                'total_receitas': total_receitas,
                'total_despesas': total_despesas,
                'saldo': saldo,
                'saldo_inicial': financial_totals.get('saldo_inicial', 0.0),
                'saldo_final': financial_totals.get('saldo_final', saldo),
            },
            'math_checks': {
                'saldo_formula_ok': not financial_totals.get('base_invalid', False) and not financial_totals.get('scale_error', False),
                'observacao': 'Saldo calculado como receitas - despesas (saldo inicial nao informado).' if not financial_totals.get('base_invalid') and not financial_totals.get('scale_error') else 'Erro na base de cálculo invalida análise financeira.',
            },
            'base_validation': {
                'base_invalid': financial_totals.get('base_invalid', False),
                'scale_error': financial_totals.get('scale_error', False),
                'base_error_message': financial_totals.get('base_error_message'),
                'scale_error_message': financial_totals.get('scale_error_message'),
            },
            'anomaly_summary': {
                'total_anomalies': int(audit_results.get('anomalies_detected', 0)),
                'anomaly_rate': float(audit_results.get('anomalies_detected', 0)) / max(1, int(audit_results.get('total_transactions', 1))),
                'high_risk_count': int(len(df[df['nivel_risco'] == 'ALTO'])),
                'medium_risk_count': int(len(df[df['nivel_risco'] == 'MÉDIO']))
            },
            'ai_performance': {
                'ai_insights': audit_results.get('ai_analysis', {}),
                'nlp_insights': audit_results.get('nlp_analysis', {}),
                'predictive_insights': audit_results.get('predictive_analysis', {})
            },
            'risk_analysis': risk_dict
        }
        
        # Gerar resumo consolidado em texto
        summary['consolidated_text_summary'] = self._generate_consolidated_text_summary(df, audit_results)
        
        return summary
    
    def _generate_consolidated_text_summary(self, df: pd.DataFrame, audit_results: Dict) -> str:
        """
        Gera um resumo consolidado em texto das 4 análises principais:
        1. Análise de IA Avançada (ai_analysis)
        2. Análise NLP (nlp_analysis)
        3. IA Preditiva (predictive_analysis)
        4. Anomalias Detectadas (anomalies_detected)
        
        Returns:
            String com resumo consolidado em texto
        """
        lines = []
        
        # Cabeçalho
        lines.append("=" * 80)
        lines.append("RESUMO CONSOLIDADO DA ANALISE")
        lines.append("=" * 80)
        lines.append("")
        
        # Informações gerais
        total_transactions = audit_results.get('total_transactions', 0)
        anomalies_detected = audit_results.get('anomalies_detected', 0)
        anomaly_rate = (anomalies_detected / max(1, total_transactions)) * 100
        
        lines.append(f"📊 INFORMAÇÕES GERAIS")
        lines.append(f"   • Total de transações analisadas: {total_transactions}")
        lines.append(f"   • Anomalias detectadas: {anomalies_detected}")
        lines.append(f"   • Taxa de anomalias: {anomaly_rate:.2f}%")
        lines.append("")
        
        # 1. Análise de IA Avançada
        lines.append("=" * 80)
        lines.append("1️⃣ ANÁLISE DE INTELIGÊNCIA ARTIFICIAL AVANÇADA")
        lines.append("=" * 80)
        ai_analysis = audit_results.get('ai_analysis', {})
        
        if ai_analysis:
            total_ai_anomalies = ai_analysis.get('total_anomalies', 0)
            high_confidence = ai_analysis.get('high_confidence_anomalies', 0)
            model_agreement = ai_analysis.get('model_agreement', {})
            
            lines.append(f"   • Anomalias detectadas por IA: {total_ai_anomalies}")
            lines.append(f"   • Anomalias de alta confiança: {high_confidence}")
            
            if model_agreement:
                both_models = model_agreement.get('both_models', 0)
                agreement_rate = model_agreement.get('agreement_rate', 0) * 100
                lines.append(f"   • Concordância entre modelos: {both_models} casos ({agreement_rate:.1f}%)")
            
            feature_importance = ai_analysis.get('feature_importance', {})
            if feature_importance:
                lines.append(f"   • Importância de características: {len(feature_importance)} características analisadas")
        else:
            lines.append("   • Nenhuma análise de IA disponível")
        
        lines.append("")
        
        # 2. Análise NLP
        lines.append("=" * 80)
        lines.append("2️⃣ ANÁLISE DE LINGUAGEM NATURAL (NLP)")
        lines.append("=" * 80)
        nlp_analysis = audit_results.get('nlp_analysis', {})
        
        if nlp_analysis:
            high_suspicion = nlp_analysis.get('high_suspicion_count', 0)
            suspicious_patterns = nlp_analysis.get('suspicious_patterns', {})
            fraud_indicators = nlp_analysis.get('fraud_indicators', {})
            recommendations = nlp_analysis.get('recommendations', [])
            
            lines.append(f"   • Transações de alta suspeição: {high_suspicion}")
            
            if suspicious_patterns:
                lines.append(f"   • Padrões suspeitos identificados: {len(suspicious_patterns)}")
                # Mostrar top 3 padrões
                sorted_patterns = sorted(suspicious_patterns.items(), key=lambda x: x[1], reverse=True)[:3]
                for pattern, count in sorted_patterns:
                    lines.append(f"     - '{pattern}': {count} ocorrência(s)")
            
            if fraud_indicators:
                lines.append(f"   • Indicadores de fraude: {len(fraud_indicators)} tipo(s) identificado(s)")
                for indicator in list(fraud_indicators.keys())[:3]:
                    lines.append(f"     - {indicator}")
            
            if recommendations:
                lines.append(f"   • Recomendações geradas: {len(recommendations)}")
                for rec in recommendations[:3]:
                    lines.append(f"     - {rec}")
        else:
            lines.append("   • Nenhuma análise NLP disponível")
        
        lines.append("")
        
        # 3. IA Preditiva
        lines.append("=" * 80)
        lines.append("3️⃣ ANÁLISE PREDITIVA E DE RISCOS")
        lines.append("=" * 80)
        predictive_analysis = audit_results.get('predictive_analysis', {})
        
        if predictive_analysis and 'error' not in predictive_analysis:
            risk_assessment = predictive_analysis.get('risk_assessment', {})
            if risk_assessment:
                overall_risk = risk_assessment.get('overall_risk_level', 'N/A')
                high_risk_transactions = risk_assessment.get('high_risk_transactions', [])
                lines.append(f"   • Nível de risco geral: {overall_risk}")
                lines.append(f"   • Transações de alto risco: {len(high_risk_transactions)}")
            
            future_risks = predictive_analysis.get('future_risks', {})
            if future_risks:
                lines.append(f"   • Riscos futuros identificados: {len(future_risks)}")
            
            predictions = predictive_analysis.get('predictions', {})
            if predictions:
                lines.append(f"   • Predições geradas: {len(predictions)}")
            
            pred_recommendations = predictive_analysis.get('recommendations', [])
            if pred_recommendations:
                lines.append(f"   • Recomendações preditivas: {len(pred_recommendations)}")
                for rec in pred_recommendations[:3]:
                    lines.append(f"     - {rec}")
        else:
            if predictive_analysis and 'error' in predictive_analysis:
                lines.append(f"   • Erro na análise preditiva: {predictive_analysis['error']}")
            else:
                lines.append("   • Nenhuma análise preditiva disponível")
        
        lines.append("")
        
        # 4. Anomalias Detectadas (Consolidadas)
        lines.append("=" * 80)
        lines.append("4️⃣ ANOMALIAS DETECTADAS (CONSOLIDADO)")
        lines.append("=" * 80)
        
        if anomalies_detected > 0:
            # Análise por nível de risco
            if 'nivel_risco' in df.columns:
                risk_series = df.loc[df['anomalia_detectada'] == True, 'nivel_risco']
                risk_counts = risk_series.value_counts() if isinstance(risk_series, pd.Series) else pd.Series(risk_series).value_counts()
                lines.append(f"   • Total de anomalias: {anomalies_detected}")
                for risk_level, count in risk_counts.items():
                    lines.append(f"     - Risco {risk_level}: {count} anomalia(s)")
            
            # Análise financeira das anomalias
            if anomalies_detected > 0:
                anomaly_df = df[df['anomalia_detectada'] == True]
                if not anomaly_df.empty and 'valor' in anomaly_df.columns:
                    total_anomaly_value = anomaly_df['valor'].sum()
                    lines.append(f"   • Valor total das transações anômalas: R$ {total_anomaly_value:,.2f}")
            
            # Taxa de anomalias
            if anomaly_rate > 10:
                lines.append(f"   ⚠️ ALERTA: Taxa de anomalias alta ({anomaly_rate:.2f}%) - investigação prioritária recomendada")
            elif anomaly_rate > 5:
                lines.append(f"   ⚠️ ATENÇÃO: Taxa de anomalias moderada ({anomaly_rate:.2f}%) - monitoramento intensificado recomendado")
            else:
                lines.append(f"   ✅ Taxa de anomalias baixa ({anomaly_rate:.2f}%) - situação normal")
        else:
            lines.append("   ✅ Nenhuma anomalia detectada nas transações analisadas")
        
        lines.append("")
        
        # Resumo Financeiro
        lines.append("=" * 80)
        lines.append("💰 RESUMO FINANCEIRO")
        lines.append("=" * 80)
        
        if 'tipo' in df.columns and 'valor' in df.columns:
            receitas_df = df[df["tipo"].str.lower() == "receita"]
            despesas_df = df[df["tipo"].str.lower() == "despesa"]
            
            total_receitas = receitas_df["valor"].sum() if not receitas_df.empty else 0
            total_despesas = despesas_df["valor"].sum() if not despesas_df.empty else 0
            saldo = total_receitas - total_despesas
            # Coerce None/NaN for display (e.g. from .sum() on missing data)
            _n = lambda x: 0 if x is None or (isinstance(x, float) and np.isnan(x)) else x
            tr, td, s = _n(total_receitas), _n(total_despesas), _n(saldo)
            lines.append(f"   • Total de receitas: R$ {tr:,.2f}")
            lines.append(f"   • Total de despesas: R$ {td:,.2f}")
            lines.append(f"   • Saldo: R$ {s:,.2f}")
            
            if s < 0:
                lines.append(f"   ⚠️ Saldo negativo - atenção necessária")
            elif s == 0:
                lines.append(f"   ⚠️ Saldo zerado - verificar transações")
            else:
                lines.append(f"   ✅ Saldo positivo")
        
        lines.append("")
        
        # Conclusão
        lines.append("=" * 80)
        lines.append("📋 CONCLUSÃO")
        lines.append("=" * 80)
        
        if anomalies_detected == 0:
            lines.append("A análise foi concluída sem detectar anomalias críticas nas transações analisadas.")
            lines.append("O sistema recomenda monitoramento contínuo para manter a integridade dos dados.")
        elif anomaly_rate > 10:
            lines.append("A análise detectou uma taxa elevada de anomalias que requer investigação imediata.")
            lines.append("Recomenda-se revisão detalhada das transações identificadas e implementação de controles adicionais.")
        elif anomaly_rate > 5:
            lines.append("A análise detectou algumas anomalias que merecem atenção.")
            lines.append("Recomenda-se monitoramento intensificado e revisão das transações de maior risco.")
        else:
            lines.append("A análise detectou poucas anomalias, indicando boa qualidade dos dados.")
            lines.append("Recomenda-se manter os controles atuais e continuar o monitoramento regular.")
        
        lines.append("")
        lines.append(f"Análise gerada em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def get_system_info(self) -> Dict[str, Any]:
        """Retorna informações do sistema avançado"""
        # Garantir que data_processing não é None (sempre inicializado no __post_init__)
        data_processing = self.config.data_processing
        if data_processing is None:
            data_processing = self.config_manager.config.data_processing
        
        return {
            'version': '3.0.0 - Advanced AI',
            'ai_engines': ['Machine Learning Ensemble', 'Natural Language Processing', 'Predictive Analytics'],
            'config_file': self.config_manager.config_file,
            'log_file': self.logger.get_log_file_path(),
            'output_directory': self.config.output_directory,
            'supported_formats': data_processing.supported_formats if data_processing else ['.csv', '.xlsx', '.xls', '.xlt', '.sxc'],
            'capabilities': [
                'Detecção de anomalias com ensemble de modelos',
                'Análise de linguagem natural',
                'Predição de riscos futuros',
                'Análise temporal avançada',
                'IA explicável (XAI)',
                'Consolidação inteligente de resultados'
            ]
        }

def main():
    """Função principal para uso via linha de comando"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sistema Avançado de Auditoria de Condomínios com IA')
    parser.add_argument('file_path', nargs='?', help='Caminho para o arquivo de dados')
    parser.add_argument('--config', help='Arquivo de configuração personalizado')
    parser.add_argument('--output-dir', help='Diretório de saída para relatórios')
    parser.add_argument('--info', action='store_true', help='Mostrar informações do sistema')
    
    args = parser.parse_args()
    
    try:
        # Inicializar sistema avançado
        audit_system = AdvancedAuditSystem(args.config)
        
        if args.info:
            # Mostrar informações do sistema
            info = audit_system.get_system_info()
            print("\n=== SISTEMA AVANÇADO DE AUDITORIA COM IA ===")
            for key, value in info.items():
                if isinstance(value, list):
                    print(f"{key}:")
                    for item in value:
                        print(f"  - {item}")
                else:
                    print(f"{key}: {value}")
            return
        
        if not args.file_path:
            parser.print_help()
            return
        
        # Executar auditoria avançada
        print(f"🚀 Iniciando auditoria avançada com IA do arquivo: {args.file_path}")
        results = audit_system.run_comprehensive_audit(args.file_path, args.output_dir)
        
        # Mostrar resultados
        if results['success']:
            print(f"\n✅ Auditoria avançada concluída com sucesso!")
            print(f"📊 Total de transações: {results['total_transactions']}")
            print(f"⚠️  Anomalias detectadas: {results['anomalies_detected']}")
            print(f"🧠 Análise de IA: {results['ai_analysis'].get('total_anomalies', 0)} anomalias")
            print(f"📝 Análise NLP: {results['nlp_analysis'].get('high_suspicion_count', 0)} suspeitas")
            print(f"📄 Relatório: {results['report_file']}")
            print(f"⏱️  Duração: {results['duration']}")
            
            # Resumo financeiro (key can exist with value None when no receita)
            summary = results['summary']
            financial = summary.get('financial_summary', {})
            _f = lambda k: financial.get(k) or 0
            print(f"\n💰 RESUMO FINANCEIRO:")
            print(f"   Receitas: R$ {_f('total_receitas'):,.2f}")
            print(f"   Despesas: R$ {_f('total_despesas'):,.2f}")
            print(f"   Saldo: R$ {_f('saldo'):,.2f}")
            
            # Resumo de anomalias
            anomaly = summary.get('anomaly_summary', {})
            print(f"\n🎯 RESUMO DE ANOMALIAS:")
            print(f"   Taxa de anomalias: {anomaly.get('anomaly_rate', 0):.1%}")
            print(f"   Alto risco: {anomaly.get('high_risk_count', 0)}")
            print(f"   Médio risco: {anomaly.get('medium_risk_count', 0)}")
        else:
            print(f"\n❌ Auditoria avançada falhou!")
            for error in results['errors']:
                print(f"   Erro: {error}")
    
    except Exception as e:
        print(f"Erro fatal: {str(e)}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
