"""WIPO technology category to CPC/IPC code mapping.

Based on the WIPO IPC-Technology Concordance table used in PATSTAT,
Google BigQuery patents-public-data, and academic patent analysis.
"""

# 35 WIPO technology fields mapped to CPC subclass prefixes.
# Source: WIPO IPC-Technology Concordance (February 2023 revision).
WIPO_TO_CPC: dict[str, list[str]] = {
    # Electrical engineering
    "Electrical machinery, apparatus, energy": [
        "F21", "H01B", "H01C", "H01F", "H01G", "H01H", "H01J",
        "H01K", "H01M", "H01R", "H01T", "H02", "H05B", "H05C",
        "H05F", "H99",
    ],
    "Audio-visual technology": [
        "G09F", "G09G", "G11B", "H04N", "H04R", "H04S", "H05K",
    ],
    "Telecommunications": [
        "H04B", "H04J", "H04K", "H04M", "H04Q", "H04W",
    ],
    "Digital communication": [
        "H04L", "H04N21",
    ],
    "Basic communication processes": [
        "H03",
    ],
    "Computer technology": [
        "G06C", "G06D", "G06E", "G06F", "G06G", "G06J", "G06K",
        "G06T", "G10L",
    ],
    "IT methods for management": [
        "G06Q",
    ],
    "Semiconductors": [
        "H01L",
    ],

    # Instruments
    "Optics": [
        "G02", "G03B", "G03C", "G03D", "G03F", "G03G", "G03H",
        "H01S",
    ],
    "Measurement": [
        "G01B", "G01C", "G01D", "G01F", "G01G", "G01H", "G01J",
        "G01K", "G01L", "G01M", "G01N", "G01P", "G01R", "G01S",
        "G01V", "G01W", "G04", "G12",
    ],
    "Analysis of biological materials": [
        "G01N33",
    ],
    "Control": [
        "G05", "G07", "G08", "G09B", "G09C", "G09D",
    ],
    "Medical technology": [
        "A61B", "A61C", "A61D", "A61F", "A61G", "A61H", "A61J",
        "A61L", "A61M", "A61N", "H05G",
    ],

    # Chemistry
    "Organic fine chemistry": [
        "C07B", "C07C", "C07D", "C07F", "C07H", "C07J", "C40B",
    ],
    "Biotechnology": [
        "C07G", "C07K", "C12M", "C12N", "C12P", "C12Q", "C12R",
        "C12S",
    ],
    "Pharmaceuticals": [
        "A61K", "A61P",
    ],
    "Macromolecular chemistry, polymers": [
        "C08B", "C08C", "C08F", "C08G", "C08H", "C08J", "C08K",
        "C08L",
    ],
    "Food chemistry": [
        "A01H", "A21", "A23B", "A23C", "A23D", "A23F", "A23G",
        "A23J", "A23K", "A23L", "C12C", "C12F", "C12G", "C12H",
        "C12J", "C13",
    ],
    "Basic materials chemistry": [
        "C01", "C02", "C03", "C04", "C05", "C06", "C08",
        "C09", "C10",
    ],
    "Materials, metallurgy": [
        "C21", "C22", "C23", "C25", "C30",
    ],
    "Surface technology, coating": [
        "B05", "B32", "C23C", "C23D", "C23F", "C23G", "C25D",
        "C25F",
    ],
    "Micro-structural and nano-technology": [
        "B81", "B82",
    ],
    "Chemical engineering": [
        "B01", "B02", "B03", "B04", "B06", "B07", "B08",
        "C40", "F25J",
    ],
    "Environmental technology": [
        "A62D", "B01D", "B09", "C02F", "F01N", "F23G", "F23J",
    ],

    # Mechanical engineering
    "Handling": [
        "B25J", "B65B", "B65C", "B65D", "B65F", "B65G", "B65H",
        "B66", "B67",
    ],
    "Machine tools": [
        "B21", "B23", "B24", "B26D", "B26F", "B27", "B30",
    ],
    "Engines, pumps, turbines": [
        "F01", "F02", "F03", "F04", "F23R",
    ],
    "Textile and paper machines": [
        "A41H", "A43D", "A46D", "B31", "C14", "D01", "D02",
        "D03", "D04", "D05", "D06", "D21",
    ],
    "Other special machines": [
        "A01B", "A01C", "A01D", "A01F", "A01G", "A01J", "A01K",
        "A01L", "A01M", "B28", "B29", "B99", "C03B", "F41",
        "F42",
    ],
    "Thermal processes and apparatus": [
        "F22", "F23", "F24", "F25", "F26", "F27", "F28",
    ],
    "Mechanical elements": [
        "F15", "F16", "F17", "G05G",
    ],
    "Transport": [
        "B60", "B61", "B62", "B63", "B64",
    ],

    # Other fields
    "Furniture, games": [
        "A47", "A63",
    ],
    "Other consumer goods": [
        "A24", "A41B", "A41C", "A41D", "A41F", "A41G", "A42",
        "A43B", "A43C", "A44", "A45", "A46B", "B42", "B43",
        "B44", "D07", "G10B", "G10C", "G10D", "G10F", "G10G",
        "G10H", "G10K",
    ],
    "Civil engineering": [
        "E01", "E02", "E03", "E04", "E05", "E06", "E21",
    ],
}


def resolve_wipo_to_cpc(categories: list[str]) -> list[str]:
    """Convert WIPO category names to CPC code prefixes.

    Returns a flat, deduplicated list of CPC prefixes. Raises ValueError
    for unrecognized category names.
    """
    cpc_prefixes = []
    for cat in categories:
        # Case-insensitive lookup
        matched = None
        for key, codes in WIPO_TO_CPC.items():
            if key.lower() == cat.lower():
                matched = codes
                break
        if matched is None:
            raise ValueError(
                f"Unknown WIPO category: '{cat}'. "
                f"Use --list-wipo-categories to see valid names."
            )
        cpc_prefixes.extend(matched)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique = []
    for code in cpc_prefixes:
        if code not in seen:
            seen.add(code)
            unique.append(code)
    return unique


def list_wipo_categories() -> list[str]:
    """Return all known WIPO category names."""
    return list(WIPO_TO_CPC.keys())
