import re, datetime as dt

SLUG_RE = re.compile(r"[^a-z0-9]+")

def slug(s:str)->str:
    "Convert arbitrary column / file names to snake_case"
    return SLUG_RE.sub("_", s.lower()).strip("_")

def infer_type(val:str):
    "quick type coercion – returns python objects not used in final code sample"
    for fn in (int, float, lambda x: dt.datetime.fromisoformat(x)):
        try: return fn(val)
        except: pass
    return val