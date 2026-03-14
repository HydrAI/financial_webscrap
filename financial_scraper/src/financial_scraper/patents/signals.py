"""Extract upstream supply chain signals from patent text.

Improved over the prototype (scripts/fetch_patents.py) with:
  - Word boundaries on all patterns
  - Case-sensitive matching for acronyms
  - New 'processes' category
  - Case-normalized deduplication
"""

import re
from dataclasses import dataclass, field

from .google_patents import PatentDetail


@dataclass
class SupplyChainSignals:
    patent_id: str = ""
    title: str = ""
    assignee: str = ""
    components: list[str] = field(default_factory=list)
    materials: list[str] = field(default_factory=list)
    companies_mentioned: list[str] = field(default_factory=list)
    frequencies: list[str] = field(default_factory=list)
    standards: list[str] = field(default_factory=list)
    processes: list[str] = field(default_factory=list)


# --- Component patterns ---
# Tuples of (pattern, flags) — use 0 for case-sensitive, re.IGNORECASE for case-insensitive.
_COMPONENT_PATTERNS: list[tuple[str, int]] = [
    # Case-sensitive acronyms (must appear as uppercase in text)
    (r'\b(?:SDR)\b', 0),
    (r'\b(?:FPGA)\b', 0),
    (r'\b(?:ASIC)\b', 0),
    (r'\b(?:GPU)\b', 0),
    (r'\b(?:CPU)\b', 0),
    (r'\b(?:PCB)\b', 0),
    (r'\b(?:LIDAR)\b', 0),
    (r'\b(?:GPS|GNSS)\b', 0),
    (r'\b(?:IMU)\b', 0),
    (r'\b(?:DAC)\b', 0),
    (r'\b(?:ADC)\b', 0),
    (r'\b(?:DSP)\b', 0),
    (r'\b(?:LNA)\b', 0),
    (r'\b(?:LCD|OLED)\b', 0),
    (r'\b(?:LED)\b', 0),
    (r'\b(?:FFT)\b', 0),
    # Case-insensitive full forms
    (r'\bsoftware[- ]defined radio\b', re.IGNORECASE),
    (r'\bfield[- ]programmable gate array\b', re.IGNORECASE),
    (r'\bapplication[- ]specific integrated circuit\b', re.IGNORECASE),
    (r'\b(?:microcontroller|microprocessor|processor)\b', re.IGNORECASE),
    (r'\b(?:antenna array|phased array|dipole antenna|omnidirectional antenna)\b', re.IGNORECASE),
    (r'\bantenna\b', re.IGNORECASE),
    (r'\bRF\s+(?:front[- ]end|amplifier|filter|receiver|transmitter|transceiver|module)\b', re.IGNORECASE),
    (r'\bprinted circuit board\b', re.IGNORECASE),
    (r'\b(?:lidar|radar module|radar sensor|radar transceiver)\b', re.IGNORECASE),
    (r'\b(?:camera|CCD|CMOS sensor|image sensor|thermal sensor|infrared sensor)\b', re.IGNORECASE),
    (r'\bglobal positioning\b', re.IGNORECASE),
    (r'\b(?:accelerometer|gyroscope|inertial measurement)\b', re.IGNORECASE),
    (r'\b(?:battery|lithium[- ]ion|power supply|power module)\b', re.IGNORECASE),
    (r'\b(?:servo|actuator|motor)\b', re.IGNORECASE),
    (r'\b(?:digital[- ]to[- ]analog|analog[- ]to[- ]digital)\b', re.IGNORECASE),
    (r'\b(?:oscillator|crystal|clock generator)\b', re.IGNORECASE),
    (r'\b(?:amplifier|low[- ]noise amplifier)\b', re.IGNORECASE),
    (r'\b(?:mixer|down[- ]converter|up[- ]converter)\b', re.IGNORECASE),
    (r'\bdigital signal process(?:or|ing)\b', re.IGNORECASE),
    (r'\b(?:neural network|machine learning|deep learning)\b', re.IGNORECASE),
    (r'\b(?:spectrometer|spectrum analyzer)\b', re.IGNORECASE),
    (r'\b(?:microphone|sound card|audio sensor)\b', re.IGNORECASE),
    (r'\b(?:speaker|transducer|buzzer)\b', re.IGNORECASE),
    (r'\b(?:display|liquid crystal display)\b', re.IGNORECASE),
    (r'\b(?:Wi-?Fi|Bluetooth|Zigbee|LoRa)\b', re.IGNORECASE),
    (r'\bFourier transform\b', re.IGNORECASE),
]

# --- Material patterns ---
_MATERIAL_PATTERNS: list[tuple[str, int]] = [
    (r'\b(?:Rogers|FR-?4|Teflon|Duroid)\b', re.IGNORECASE),
    (r'\b(?:aluminum|aluminium|titanium|carbon fiber|composite)\b', re.IGNORECASE),
    (r'\b(?:silicon|gallium arsenide)\b', re.IGNORECASE),
    (r'\bGaAs\b', 0),
    (r'\bGaN\b', 0),
    (r'\bSiGe\b', 0),
    (r'\b(?:copper|brass|ferrite)\b', re.IGNORECASE),
    (r'\b(?:polyethylene|polycarbonate|nylon)\b', re.IGNORECASE),
    (r'\bABS\b', 0),
    (r'\bsteel\b', re.IGNORECASE),
]

# --- Company/brand patterns ---
_COMPANY_PATTERNS: list[tuple[str, int]] = [
    (r'\b(?:Intel|AMD|NVIDIA|Xilinx|Altera|Qualcomm)\b', 0),
    (r'\bTexas Instruments\b', 0),
    (r'\b(?:Analog Devices|NXP|Microchip|STMicroelectronics|Infineon)\b', 0),
    (r'\b(?:Broadcom|MediaTek|Renesas|ON Semiconductor|Marvell)\b', 0),
    (r'\b(?:National Instruments|Keysight|Rohde & Schwarz)\b', 0),
    (r'\b(?:Ettus Research|USRP|HackRF|Lime Microsystems)\b', 0),
    (r'\bRTL-SDR\b', 0),
    (r'\b(?:Raspberry Pi|Arduino|Jetson|Coral|Movidius)\b', 0),
    (r'\b(?:FLIR|Teledyne|Leonardo|Bosch|Honeywell|Raytheon|L3Harris)\b', 0),
    (r'\b(?:Saab|Thales|BAE Systems|Northrop Grumman|Lockheed Martin)\b', 0),
    (r'\b(?:DJI|Parrot|Yuneec|Autel)\b', 0),
    (r'\b(?:MATLAB|LabVIEW|GNU Radio|TensorFlow|PyTorch)\b', 0),
]

# --- Frequency band patterns ---
_FREQUENCY_PATTERNS: list[tuple[str, int]] = [
    (r'\b\d+(?:\.\d+)?\s*(?:MHz|GHz|kHz)\b', re.IGNORECASE),
    (r'\b\d{2,}\s*Hz\b', 0),  # Only match numbers >= 10 Hz to avoid "0 Hz" false positives
    (r'\b(?:ISM band|LTE|5G|cellular)\b', re.IGNORECASE),
    (r'\b(?:L-?band|S-?band|C-?band|X-?band|Ku-?band|Ka-?band)\b', re.IGNORECASE),
]

# --- Standards/protocols patterns ---
_STANDARDS_PATTERNS: list[tuple[str, int]] = [
    (r'\bIEEE\s+802\.\d+\w*\b', 0),
    (r'\bMIL-STD-\d+\w*\b', 0),
    (r'\bDO-\d+\w*\b', 0),
    (r'\bSTANAG\s+\d+\b', 0),
    (r'\b(?:TCP/IP|UDP|MQTT|gRPC|SAPIENT)\b', 0),
    (r'\b(?:IP67|IP68|MIL-SPEC|NDAA|TAA|ITAR)\b', 0),
]

# --- Manufacturing process patterns (NEW) ---
_PROCESS_PATTERNS: list[tuple[str, int]] = [
    (r'\b(?:injection mold(?:ing|ed))\b', re.IGNORECASE),
    (r'\b(?:surface mount|SMT|SMD)\b', re.IGNORECASE),
    (r'\b(?:die cast(?:ing)?|die-cast(?:ing)?)\b', re.IGNORECASE),
    (r'\b(?:CNC machin(?:ing|ed))\b', re.IGNORECASE),
    (r'\b(?:3D print(?:ing|ed)|additive manufactur(?:ing|ed))\b', re.IGNORECASE),
    (r'\b(?:anodiz(?:ing|ed)|electroplat(?:ing|ed))\b', re.IGNORECASE),
    (r'\b(?:solder(?:ing|ed)|reflow solder)\b', re.IGNORECASE),
    (r'\b(?:wave solder(?:ing)?)\b', re.IGNORECASE),
    (r'\b(?:conformal coat(?:ing)?)\b', re.IGNORECASE),
    (r'\b(?:wire bond(?:ing)?|flip[- ]chip)\b', re.IGNORECASE),
    (r'\b(?:photolithography|etching|lithograph(?:y|ic))\b', re.IGNORECASE),
    (r'\b(?:sintering|forging|stamping|extrusion)\b', re.IGNORECASE),
]


def _run_patterns(
    text: str, patterns: list[tuple[str, int]]
) -> list[str]:
    """Run a list of (regex, flags) patterns against text, return unique matches."""
    matches = []
    for pat, flags in patterns:
        found = re.findall(pat, text, flags)
        matches.extend(m.strip() for m in found if m.strip())
    return matches


def _deduplicate_case_normalized(items: list[str]) -> list[str]:
    """Deduplicate preserving the first-seen casing of each term.

    "ASIC" and "asic" are treated as the same; the first occurrence wins.
    """
    seen: dict[str, str] = {}
    for item in items:
        key = item.lower()
        if key not in seen:
            seen[key] = item
    return sorted(seen.values(), key=lambda s: s.lower())


def extract_signals(patent: PatentDetail) -> SupplyChainSignals:
    """Extract supply chain signals from patent text."""
    signals = SupplyChainSignals(
        patent_id=patent.patent_id,
        title=patent.title,
        assignee=patent.assignee,
    )

    text = "\n".join([patent.abstract, patent.full_text])
    if not text.strip():
        return signals

    signals.components = _deduplicate_case_normalized(
        _run_patterns(text, _COMPONENT_PATTERNS)
    )
    signals.materials = _deduplicate_case_normalized(
        _run_patterns(text, _MATERIAL_PATTERNS)
    )
    signals.companies_mentioned = _deduplicate_case_normalized(
        _run_patterns(text, _COMPANY_PATTERNS)
    )
    signals.frequencies = _deduplicate_case_normalized(
        _run_patterns(text, _FREQUENCY_PATTERNS)
    )
    signals.standards = _deduplicate_case_normalized(
        _run_patterns(text, _STANDARDS_PATTERNS)
    )
    signals.processes = _deduplicate_case_normalized(
        _run_patterns(text, _PROCESS_PATTERNS)
    )

    return signals
