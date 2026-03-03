"""
One-off script to split app/extraction/legacy/data_processor.py into
text_utils, normalizer, loader, quality.
"""
import os

LEGACY_DIR = os.path.join(os.path.dirname(__file__), "..", "app", "extraction", "legacy")
DP_PATH = os.path.join(LEGACY_DIR, "data_processor.py")

with open(DP_PATH, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Line numbers are 1-based; we use 0-based for slicing
def get_lines(start, end):
    return "".join(lines[start:end])

# --- text_utils.py: lines 1-935 (through _parse_valor_monetario)
text_utils_content = get_lines(0, 935)
# Remove unused imports (io, Tuple if not used)
text_utils_content = text_utils_content.replace(
    "from typing import Dict, List, Optional, Tuple, Union, Any",
    "from typing import Dict, List, Optional, Any"
)
text_utils_content = text_utils_content.replace("import io\n", "")
with open(os.path.join(LEGACY_DIR, "text_utils.py"), "w", encoding="utf-8") as f:
    f.write(text_utils_content)
print("Wrote text_utils.py")

# --- normalizer.py: _try_normalize_balancete through _try_normalize_pdf_mixed_text (936-1098), then clean_data (1971-2226), categorize_transactions (2227-2262)
normalizer_imports = '''"""
Normalização de dados: balancete, PDF misto, clean_data, categorize_transactions.
"""
import re
import pandas as pd
from datetime import datetime
from typing import Optional

from . import text_utils
from .text_utils import (
    _parse_valor_monetario,
    _RE_VALOR_BR,
    _RE_VALOR_BR_COM_SINAL,
    _BALANCETE_CONTA,
    _BALANCETE_CREDITO,
    _BALANCETE_DEBITO,
    extract_condominio_name,
    extract_period_from_text,
)

'''
normalizer_body1 = get_lines(935, 1098)  # _try_normalize_balancete through _try_normalize_pdf_mixed_text
# Fix references: _parse_valor_monetario, _BALANCETE_* are now from text_utils
normalizer_body1 = normalizer_body1.replace("_parse_valor_monetario(", "text_utils._parse_valor_monetario(")
normalizer_body1 = normalizer_body1.replace("_BALANCETE_CONTA", "text_utils._BALANCETE_CONTA")
normalizer_body1 = normalizer_body1.replace("_BALANCETE_CREDITO", "text_utils._BALANCETE_CREDITO")
normalizer_body1 = normalizer_body1.replace("_BALANCETE_DEBITO", "text_utils._BALANCETE_DEBITO")
normalizer_body1 = normalizer_body1.replace("_RE_VALOR_BR.", "text_utils._RE_VALOR_BR.")
normalizer_body1 = normalizer_body1.replace("_RE_VALOR_BR.search", "text_utils._RE_VALOR_BR.search")
normalizer_body1 = normalizer_body1.replace("_RE_VALOR_BR_COM_SINAL.finditer", "text_utils._RE_VALOR_BR_COM_SINAL.finditer")
normalizer_body1 = normalizer_body1.replace("_parse_valor_monetario(valor_str", "text_utils._parse_valor_monetario(valor_str")

normalizer_body2 = get_lines(1970, 2262)  # clean_data through categorize_transactions
normalizer_body2 = normalizer_body2.replace("extract_condominio_name(df)", "text_utils.extract_condominio_name(df)")
normalizer_body2 = normalizer_body2.replace("extract_period_from_text(first_text)", "text_utils.extract_period_from_text(first_text)")
normalizer_body2 = normalizer_body2.replace("_try_normalize_balancete(df)", "_try_normalize_balancete(df)")
normalizer_body2 = normalizer_body2.replace("_try_normalize_pdf_mixed_text(df)", "_try_normalize_pdf_mixed_text(df)")
normalizer_body2 = normalizer_body2.replace("_parse_valor_monetario(", "text_utils._parse_valor_monetario(")

normalizer_content = normalizer_imports + normalizer_body1 + "\n" + normalizer_body2
with open(os.path.join(LEGACY_DIR, "normalizer.py"), "w", encoding="utf-8") as f:
    f.write(normalizer_content)
print("Wrote normalizer.py")

# --- quality.py: check_extraction_quality only
quality_content = '''"""
Qualidade da extração: check_extraction_quality.
"""
import pandas as pd
from typing import Optional

from .text_utils import extract_condominio_name

''' + get_lines(1872, 1968)
quality_content = quality_content.replace("extract_condominio_name(df)", "extract_condominio_name(df)")
with open(os.path.join(LEGACY_DIR, "quality.py"), "w", encoding="utf-8") as f:
    f.write(quality_content)
print("Wrote quality.py")

# --- loader.py: from _LOAD_DOCUMENT_MAX_TEXT_LEN through load_data (1099-1870), and load_document/load_document_from_bytes/load_data (1715-1870 are inside that range)
# So loader = lines 1099-1870 (constants, _parse_ocr_text_to_transactions, _try_ocr_pdf_text, _load_pdf_to_dataframe, _get_pdf_text_and_page_info, _HOLERITE_URL_KEYWORDS, extract_hyperlinks_*, _detect_*, load_document, load_document_from_bytes, load_data)
loader_body = get_lines(1098, 1870)
# Add imports at top
loader_imports = '''"""
Loader: carregamento de documentos (PDF, Excel, ODS, CSV), OCR, hyperlinks, load_document/load_data.
"""
import io
import os
import re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from . import normalizer
from . import text_utils
from .normalizer import _try_normalize_pdf_mixed_text
from .text_utils import _parse_valor_monetario, _RE_VALOR_BR_COM_SINAL
from .ocr_preprocessor import extract_text_with_ocr, parse_ocr_text_to_dataframe, preprocess_scanned_pdf

'''
# In loader_body, replace references: _try_normalize_pdf_mixed_text -> already imported; _parse_valor_monetario, _RE_VALOR_BR_COM_SINAL -> from text_utils; extract_text_with_ocr etc from .ocr_preprocessor
loader_body = loader_body.replace("_try_normalize_pdf_mixed_text(", "_try_normalize_pdf_mixed_text(")
loader_body = loader_body.replace("from .ocr_preprocessor import extract_text_with_ocr", "# from .ocr_preprocessor (top)")
loader_body = loader_body.replace("from .ocr_preprocessor import parse_ocr_text_to_dataframe", "# from .ocr_preprocessor (top)")
loader_body = loader_body.replace("from .ocr_preprocessor import preprocess_scanned_pdf", "# from .ocr_preprocessor (top)")
loader_content = loader_imports + loader_body
with open(os.path.join(LEGACY_DIR, "loader.py"), "w", encoding="utf-8") as f:
    f.write(loader_content)
print("Wrote loader.py")
print("Done. Fix any remaining cross-references by hand if needed.")
