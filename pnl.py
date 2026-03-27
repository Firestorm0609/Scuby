"""
pnl.py — PnL card generator for Scooby OG Finder.
Requires: pip install Pillow

Layout (Rick-bot style):
  Top    : SCOOBY OG FINDER brand + date
  Upper  : CODED @ $6.3K  /  35m ago · @username
  Centre : +1400.0%  (giant, auto-sized, glow effect)
  Lower  : Reached $94.5K  /  price entry → exit
  Footer : CA in groups

Visual: deep navy gradient, gold border, gold shimmer dust,
        subtle paw-print watermarks, green/red PnL glow.
"""

import io, math, random
from datetime import datetime, timezone

try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

W, H = 1080, 680

# ── Palette ───────────────────────────────────────────────────────────────────
GOLD   = (212, 175,  55)
GOLDD  = (120,  90,  20)
GOLDE  = (255, 220, 100)
WHITE  = (255, 255, 255)
OFF_W  = (215, 215, 230)
DIM    = (140, 140, 165)
GREY   = ( 80,  80, 105)
GREEN  = ( 45, 220, 110)
GREEND = ( 15, 130,  60)
RED    = (230,  65,  65)
REDD   = (150,  25,  25)

# ── Font paths ────────────────────────────────────────────────────────────────
_PR  = ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arial.ttf"]
_PB  = ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "C:/Windows/Fonts/arialbd.ttf"]
_PM  = ["/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf",
        "C:/Windows/Fonts/cour.ttf"]


def _font(size, bold=False, mono=False):
    if not PIL_AVAILABLE:
        return None
    for p in (_PM if mono else (_PB if bold else _PR)):
        try:
            return ImageFont.truetype(p, size)
        except (IOError, OSError):
            continue
    return ImageFont.load_default()


def _tw(d, t, f):
    bb = d.textbbox((0, 0), t, font=f)
    return bb[2] - bb[0]

def _th(d, t, f):
    bb = d.textbbox((0, 0), t, font=f)
    return bb[3] - bb[1]

def _cx(d, t, f, y, col, w=W):
    d.text(((w - _tw(d, t, f)) // 2, y), t, font=f, fill=col)


def _fmt_mcap(v):
    if v >= 1e9: return f"${v/1e9:.2f}B"
    if v >= 1e6: return f"${v/1e6:.2f}M"
    if v >= 1e3: return f"${v/1e3:.1f}K"
    return f"${v:.0f}"

def _fmt_price(p):
    if p == 0:      return "$0"
    if p >= 100:    return f"${p:,.2f}"
    if p >= 1:      return f"${p:.4f}"
    if p >= 0.001:  return f"${p:.6f}"
    return f"${p:.10f}".rstrip("0")

def _elapsed(ts):
    import time
    s = time.time() - ts
    if s < 3600:   return f"{int(s/60)}m ago"
    if s < 86400:  return f"{s/3600:.1f}h ago"
    return f"{int(s/86400)}d ago"


# ── Background helpers ────────────────────────────────────────────────────────

def _make_bg():
    img = Image.new("RGB", (W, H))
    px  = img.load()
    for y in range(H):
        fy = y / H
        for x in range(W):
            fx = x / W
            r = int(9 * (1-fy) + 5 * fy)
            g = int(8 * (1-fy) + 4 * fy)
            b = int(22 * (1-fy) + 12 * fy)
            dx = (fx - 0.5) * 2
            dy = (fy - 0.5) * 2
            lift = max(0, 1 - math.sqrt(dx*dx + dy*dy) * 1.1) * 10
            px[x, y] = (min(255, r + int(lift)),
                        min(255, g + int(lift)),
                        min(255, b + int(lift * 2)))
    return img


def _add_shimmer(draw, seed=99):
    random.seed(seed)
    # Gold dust particles
    for _ in range(260):
        sx = random.randint(0, W)
        sy = random.randint(0, H)
        sr = random.randint(1, 2)
        a  = random.randint(8, 40)
        gv = (random.randint(180, 255), random.randint(140, 200), random.randint(20, 60))
        draw.ellipse([sx-sr, sy-sr, sx+sr, sy+sr], fill=(*gv, a))
    # Short gold streaks
    for _ in range(14):
        sx = random.randint(0, W)
        sy = random.randint(0, H)
        ex = sx + random.randint(30, 100)
        ey = sy + random.randint(-8, 8)
        draw.line([(sx, sy), (ex, ey)], fill=(*GOLD, random.randint(6, 18)), width=1)


def _add_paws(draw, seed=7):
    random.seed(seed)

    def paw(cx, cy, size=40, alpha=9):
        c = (*GOLD, alpha)
        pw = int(size * .55)
        ph = int(size * .50)
        draw.ellipse([cx-pw, cy, cx+pw, cy+ph*2], fill=c)
        tr = int(size * .16)
        for ox, oy in [(-int(size*.35), -int(size*.38)),
                       (-int(size*.12), -int(size*.52)),
                       ( int(size*.12), -int(size*.52)),
                       ( int(size*.35), -int(size*.38))]:
            draw.ellipse([cx+ox-tr, cy+oy-tr, cx+ox+tr, cy+oy+tr], fill=c)

    spots = [
        ( 90,  90, 36, 8), (W-90, 100, 32, 7),
        ( 60, H-100, 38, 8), (W-70, H-90, 34, 7),
        (W//2-200, H-130, 30, 6), (W//2+200,  55, 28, 6),
        (180, H//2+80,  26, 5),   (W-160, H//2-40, 28, 5),
    ]
    for pcx, pcy, psz, pa in spots:
        paw(pcx, pcy, psz, pa)


# ── Main generator ────────────────────────────────────────────────────────────

def generate_pnl_card(
    symbol: str,
    name: str,
    ca: str,
    entry_mcap: float,
    current_mcap: float,
    entry_price: float,
    current_price: float,
    entry_ts: float,
    scanned_by: str = "",
) -> io.BytesIO:
    """
    Generate a PnL card (Rick-bot layout, dark-gold style).
    Returns an in-memory PNG as io.BytesIO.

    Args:
        symbol        : token ticker, e.g. "BONK"
        name          : full token name
        ca            : Solana contract address
        entry_mcap    : mcap at first sniff (USD)
        current_mcap  : live mcap (USD)
        entry_price   : price at first sniff
        current_price : live price
        entry_ts      : unix timestamp of first sniff
        scanned_by    : username who first sniffed, e.g. "@raggy"

    Raises:
        ImportError if Pillow is not installed
        ValueError  if entry_mcap <= 0
    """
    if not PIL_AVAILABLE:
        raise ImportError("Pillow not installed — run: pip install Pillow")
    if entry_mcap <= 0:
        raise ValueError("entry_mcap must be > 0")

    multiple  = current_mcap / entry_mcap
    pct       = (multiple - 1) * 100
    is_profit = pct >= 0
    pnl_col   = GREEN  if is_profit else RED
    pnl_drk   = GREEND if is_profit else REDD
    sign      = "+" if is_profit else ""
    pnl_str   = f"{sign}{pct:.1f}%"
    multi_str = f"{multiple:.1f}x"
    elapsed   = _elapsed(entry_ts)

    # ── Build background ──────────────────────────────────────────────────────
    img  = _make_bg()
    draw = ImageDraw.Draw(img, "RGBA")
    _add_shimmer(draw)
    _add_paws(draw)

    # ── Border ────────────────────────────────────────────────────────────────
    draw.rectangle([0, 0, W-1, H-1], outline=(*GOLD, 255), width=3)
    draw.rectangle([3, 3, W-4,  H-4], outline=(*GOLD,  60), width=1)
    draw.rectangle([5, 5, W-6,  H-6], outline=(*GOLD,  25), width=1)

    # Top shimmer strip
    for i in range(5):
        a   = int(200 * (1 - i/5))
        col = (int(GOLD[0] + (GOLDE[0]-GOLD[0]) * (1-i/5)),
               int(GOLD[1] + (GOLDE[1]-GOLD[1]) * (1-i/5)),
               int(GOLD[2] + (GOLDE[2]-GOLD[2]) * (1-i/5)))
        draw.rectangle([3, i+3, W-3, i+4], fill=(*col, a))

    # ── Header ────────────────────────────────────────────────────────────────
    hf = _font(20, bold=True)
    draw.text((32, 18), "🐾  SCOOBY OG FINDER", font=hf, fill=GOLD)
    datef = _font(15)
    date_str = datetime.now(timezone.utc).strftime("%b %d, %Y")
    draw.text((W-32-_tw(draw, date_str, datef), 22), date_str, font=datef, fill=(*GREY, 180))
    draw.rectangle([32, 50, W-32, 51], fill=(*GOLD, 28))

    # ── Token name ────────────────────────────────────────────────────────────
    sf  = _font(58, bold=True)
    sym = f"${symbol}"
    sxp = (W - _tw(draw, sym, sf)) // 2
    draw.text((sxp+2, 58), sym, font=sf, fill=(*GOLDD, 100))   # shadow
    draw.text((sxp,   56), sym, font=sf, fill=WHITE)
    nmf = _font(19)
    _cx(draw, name, nmf, 122, GREY)

    # ── CODED @ panel ─────────────────────────────────────────────────────────
    PY1 = 148
    draw.rounded_rectangle([32, PY1, W-32, PY1+76], radius=8,
                            fill=(255,255,255, 4), outline=(*GOLD, 18), width=1)

    cf         = _font(38, bold=True)
    lbl_coded  = "CODED @ "
    val_coded  = _fmt_mcap(entry_mcap)
    full_cw    = _tw(draw, lbl_coded, cf) + _tw(draw, val_coded, cf)
    fxp        = (W - full_cw) // 2
    draw.text((fxp,                             PY1+8), lbl_coded, font=cf, fill=GOLD)
    draw.text((fxp + _tw(draw, lbl_coded, cf),  PY1+8), val_coded, font=cf, fill=OFF_W)

    sub = f"{elapsed}  ·  {scanned_by}" if scanned_by else elapsed
    _cx(draw, sub, _font(18), PY1+50, DIM)

    # ── Giant PnL ─────────────────────────────────────────────────────────────
    for sz in (195, 170, 148, 125, 105, 88):
        pf = _font(sz, bold=True)
        if _tw(draw, pnl_str, pf) <= W - 48:
            break

    pnl_h = _th(draw, pnl_str, pf)
    py_   = 238
    pxp   = (W - _tw(draw, pnl_str, pf)) // 2

    draw.text((pxp+3, py_+4), pnl_str, font=pf, fill=(*pnl_drk, 180))
    draw.text((pxp+1, py_+2), pnl_str, font=pf, fill=(*pnl_drk,  80))
    draw.text((pxp,   py_),   pnl_str, font=pf, fill=pnl_col)

    # Multiplier pill
    mf  = _font(28, bold=True)
    mw  = _tw(draw, multi_str, mf) + 28
    mh  = _th(draw, multi_str, mf) + 12
    mxp = (W - mw) // 2
    myp = py_ + pnl_h + 8
    draw.rounded_rectangle([mxp, myp, mxp+mw, myp+mh], radius=mh//2,
                            fill=(*pnl_col, 20), outline=(*pnl_col, 110), width=2)
    draw.text((mxp+14, myp+6), multi_str, font=mf, fill=pnl_col)

    # ── REACHED panel ─────────────────────────────────────────────────────────
    PY2 = myp + mh + 16
    draw.rounded_rectangle([32, PY2, W-32, PY2+76], radius=8,
                            fill=(255,255,255, 4), outline=(*GOLD, 18), width=1)

    rf        = _font(38, bold=True)
    lbl_reach = "Reached "
    val_reach = _fmt_mcap(current_mcap)
    rw2       = _tw(draw, lbl_reach, rf) + _tw(draw, val_reach, rf)
    rxp       = (W - rw2) // 2
    draw.text((rxp,                              PY2+8), lbl_reach, font=rf, fill=GOLD)
    draw.text((rxp + _tw(draw, lbl_reach, rf),   PY2+8), val_reach, font=rf, fill=WHITE)

    price_str = f"{_fmt_price(entry_price)}  →  {_fmt_price(current_price)}"
    _cx(draw, price_str, _font(17), PY2+54, GREY)

    # ── Footer CA ─────────────────────────────────────────────────────────────
    draw.rectangle([32, H-42, W-32, H-41], fill=(*GOLD, 22))
    mof    = _font(14, mono=True)
    groups = [ca[i:i+4] for i in range(0, min(len(ca), 32), 4)]
    _cx(draw, "  ".join(groups), mof, H-32, (*GREY, 150))

    # ── Encode ────────────────────────────────────────────────────────────────
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf
