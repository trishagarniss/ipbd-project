import logging

log = logging.getLogger(__name__)

ISPU_BREAKS = [0, 50, 100, 200, 300, 500]

POLLUTANT_TABLES = {
    "pm25": [0, 15.5, 55.4, 150.4, 250.4, 500],
    "pm10": [0, 50, 150, 350, 420, 500],
    "so2":   [0, 52, 180, 400, 800, 1200],
    "co":    [0, 4000, 8000, 15000, 30000, 45000],
    "o3":    [0, 120, 235, 400, 800, 1000],
    "no2":   [0, 80, 200, 1130, 2260, 3000],
}


def _sub_index(conc, breakpoints):
    if conc is None:
        return None
    if conc <= 0:
        return 0.0
    for i in range(len(breakpoints) - 1):
        lo, hi = breakpoints[i], breakpoints[i + 1]
        if lo < conc <= hi:
            frac = (conc - lo) / (hi - lo)
            return ISPU_BREAKS[i] + (ISPU_BREAKS[i + 1] - ISPU_BREAKS[i]) * frac
    if conc > breakpoints[-1]:
        return float(ISPU_BREAKS[-1])
    return 0.0


def compute_ispu(pm25=None, pm10=None, no2=None, so2=None, co=None, o3=None):
    vals = {
        "pm25": _sub_index(pm25, POLLUTANT_TABLES["pm25"]),
        "pm10": _sub_index(pm10, POLLUTANT_TABLES["pm10"]),
        "no2":  _sub_index(no2,  POLLUTANT_TABLES["no2"]),
        "so2":  _sub_index(so2,  POLLUTANT_TABLES["so2"]),
        "co":   _sub_index(co,   POLLUTANT_TABLES["co"]),
        "o3":   _sub_index(o3,   POLLUTANT_TABLES["o3"]),
    }
    valid = [v for v in vals.values() if v is not None]
    if not valid:
        return None, None
    ispu = max(valid)
    return round(ispu, 1), _ispu_category(ispu)


def _ispu_category(ispu):
    if ispu is None:
        return None
    if ispu <= 50:
        return "Baik"
    if ispu <= 100:
        return "Sedang"
    if ispu <= 200:
        return "Tidak Sehat"
    if ispu <= 300:
        return "Sangat Tidak Sehat"
    return "Berbahaya"
