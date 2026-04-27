from __future__ import annotations

import argparse
import re
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    FrameBreak,
    HRFlowable,
    Image,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.flowables import KeepTogether
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
from reportlab.graphics import renderPDF


ROOT = Path(__file__).resolve().parents[1]


# ── Pipeline diagram renderer ─────────────────────────────────────────────────

def _draw_pipeline_diagram(col_w: float) -> Drawing:
    """
    Draw both pipeline topologies as a proper box-and-arrow figure.
    Returns a ReportLab Drawing flowable.
    Layout is calculated to fit within col_w.
    """
    BG   = colors.HexColor("#f5f5f5")
    BORD = colors.HexColor("#aaaaaa")
    NODE = colors.HexColor("#ddeeff")
    NLIN = colors.HexColor("#5588bb")
    TXT  = colors.black
    ARR  = colors.HexColor("#333333")

    W  = col_w - 8        # usable drawing width (~242pt for a single column)
    H  = 218
    M  = 5                # left/right margin inside drawing
    d  = Drawing(W, H)
    d.add(Rect(0, 0, W, H, fillColor=BG, strokeColor=BORD, strokeWidth=0.5))

    # Node dimensions — sized so 4 nodes + 3 gaps fit within (W - 2*M)
    GAP = 4
    nw  = int((W - 2*M - 3*GAP) / 4)   # ~54pt per node
    nh  = 13
    fs  = 5.2

    def node(x, y, lbl):
        d.add(Rect(x, y, nw, nh, fillColor=NODE, strokeColor=NLIN, strokeWidth=0.5))
        d.add(String(x + nw/2, y + 3.5, lbl,
                     fontName='Helvetica', fontSize=fs,
                     fillColor=TXT, textAnchor='middle'))

    def harrow(x1, x2, y):
        d.add(Line(x1, y, x2, y, strokeColor=ARR, strokeWidth=0.8))
        ah = 3.5
        d.add(Polygon([x2, y, x2-ah, y+ah*0.4, x2-ah, y-ah*0.4],
                      fillColor=ARR, strokeColor=ARR, strokeWidth=0))

    def varrow(x, y1, y2):
        d.add(Line(x, y1, x, y2, strokeColor=ARR, strokeWidth=0.8))
        ah = 3.5
        if y2 < y1:   # pointing down
            d.add(Polygon([x, y2, x-ah*0.4, y2+ah, x+ah*0.4, y2+ah],
                          fillColor=ARR, strokeColor=ARR, strokeWidth=0))
        else:          # pointing up
            d.add(Polygon([x, y2, x-ah*0.4, y2-ah, x+ah*0.4, y2-ah],
                          fillColor=ARR, strokeColor=ARR, strokeWidth=0))

    def hline(x1, x2, y):
        d.add(Line(x1, y, x2, y, strokeColor=ARR, strokeWidth=0.8))

    def vline(x, y1, y2):
        d.add(Line(x, y1, x, y2, strokeColor=ARR, strokeWidth=0.8))

    def lbl(x, y, txt, anchor='middle', size=5, bold=False):
        fn = 'Helvetica-Bold' if bold else 'Helvetica'
        d.add(String(x, y, txt, fontName=fn, fontSize=size,
                     fillColor=TXT, textAnchor=anchor))

    step = nw + GAP

    # ── (a) NYC Taxi — 8-node sequential chain ────────────────────────────────
    y0 = H - 28
    lbl(M, y0 + nh + 6,
        '(a) NYC Yellow/Green Taxi — 8-node sequential chain:',
        anchor='start', size=4.8, bold=True)

    xs = [M + i * step for i in range(4)]
    for x, nm in zip(xs, ['raw_trips', 'trips_valid', 'trips_enriched', 'trips_classified']):
        node(x, y0, nm)
    for i in range(3):
        harrow(xs[i]+nw, xs[i+1], y0+nh/2)

    # zone_lookup feeds UP into trips_enriched
    ze_cx = xs[2] + nw/2          # centre of trips_enriched
    zy = y0 - 22
    zx = ze_cx - nw/2
    node(zx, zy, 'zone_lookup')
    varrow(ze_cx, zy + nh, y0)    # arrow pointing UP

    # trips_classified fans DOWN to 3 mart nodes spread across full width
    fan_y = zy - 22
    fan_cx = [M + nw/2, W/2, W - M - nw/2]   # 3 centres spread across W
    fan_nm = ['daily_zone', 'fare_band', 'peak_hour']
    for cx, nm in zip(fan_cx, fan_nm):
        node(cx - nw/2, fan_y, nm)

    src_cx = xs[3] + nw/2
    bus_y  = fan_y + nh + 6       # horizontal bus above fan nodes
    vline(src_cx, y0, bus_y)
    hline(fan_cx[0], fan_cx[-1], bus_y)
    for cx in fan_cx:
        varrow(cx, bus_y, fan_y + nh)

    # ── (b) BTS Airline — 8-node dual-path DAG ───────────────────────────────
    y1 = fan_y - 32
    lbl(M, y1 + nh + 6,
        '(b) BTS Airline — 8-node dual-path DAG:',
        anchor='start', size=4.8, bold=True)

    xb = [M + i * step for i in range(4)]
    for x, nm in zip(xb, ['raw_flights', 'flights_valid', 'flights_enriched', 'flights_classified']):
        node(x, y1, nm)
    for i in range(3):
        harrow(xb[i]+nw, xb[i+1], y1+nh/2)

    # airport_lookup feeds flights_enriched AND route_delay
    al_cx = xb[2] + nw/2          # below flights_enriched
    al_y  = y1 - 22
    node(al_cx - nw/2, al_y, 'airport_lookup')
    varrow(al_cx, al_y + nh, y1)  # UP to flights_enriched

    # route_delay (bottom left)
    rd_y = al_y - 22
    node(M, rd_y, 'route_delay')
    rd_cx = M + nw/2
    hline(rd_cx, al_cx, al_y + nh/2)
    varrow(rd_cx, al_y + nh/2, rd_y + nh)

    # Spread 3 output nodes (route_delay, carrier, delay_tier) evenly across W
    # route_delay already placed at M; carrier and delay_tier go right
    ct_y  = al_y - 22
    out_spacing = (W - 2*M - 3*nw) / 2      # even spacing between 3 nodes
    carrier_x  = M + nw + out_spacing        # centre column
    delt_x     = M + 2*nw + 2*out_spacing   # right column
    node(carrier_x, ct_y,  'carrier')
    node(delt_x,    ct_y,  'delay_tier')

    # flights_classified → carrier and delay_tier via horizontal bus
    fc_cx  = xb[3] + nw/2
    bus2_y = ct_y + nh + 5
    vline(fc_cx, y1, bus2_y)
    hline(carrier_x + nw/2, delt_x + nw/2, bus2_y)
    varrow(carrier_x + nw/2, bus2_y, ct_y + nh)
    varrow(delt_x    + nw/2, bus2_y, ct_y + nh)

    # Figure caption
    cap_y = rd_y - 9
    lbl(W/2, cap_y,
        'Fig. 1: Pipeline topologies. Source nodes (left) → staging → mart nodes (right/bottom).',
        anchor='middle', size=4.3)

    return d

# ── Page geometry (IEEE two-column style on Letter) ──────────────────────────
PAGE_W, PAGE_H = LETTER            # 612 × 792 pt
MARGIN_T = 0.75 * inch
MARGIN_B = 1.00 * inch
MARGIN_L = 0.65 * inch
MARGIN_R = 0.65 * inch
BODY_W   = PAGE_W - MARGIN_L - MARGIN_R          # 7.20 in
BODY_H   = PAGE_H - MARGIN_T - MARGIN_B          # 9.25 in
GUTTER   = 0.25 * inch
COL_W    = (BODY_W - GUTTER) / 2                 # 3.475 in


def _frames_first_page(header_h: float):
    """Three frames: full-width header + two columns below on page 1."""
    col_h = BODY_H - header_h - 0.12 * inch
    header = Frame(
        MARGIN_L, PAGE_H - MARGIN_T - header_h,
        BODY_W, header_h,
        leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0,
        id="header",
    )
    left = Frame(
        MARGIN_L, MARGIN_B,
        COL_W, col_h,
        leftPadding=0, rightPadding=4, topPadding=0, bottomPadding=0,
        id="col_left_p1",
    )
    right = Frame(
        MARGIN_L + COL_W + GUTTER, MARGIN_B,
        COL_W, col_h,
        leftPadding=4, rightPadding=0, topPadding=0, bottomPadding=0,
        id="col_right_p1",
    )
    return header, left, right


def _frames_later_page():
    """Two equal-height column frames for body pages."""
    left = Frame(
        MARGIN_L, MARGIN_B,
        COL_W, BODY_H,
        leftPadding=0, rightPadding=4, topPadding=0, bottomPadding=0,
        id="col_left",
    )
    right = Frame(
        MARGIN_L + COL_W + GUTTER, MARGIN_B,
        COL_W, BODY_H,
        leftPadding=4, rightPadding=0, topPadding=0, bottomPadding=0,
        id="col_right",
    )
    return left, right


def _add_header_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("Times-Roman", 8)
    canvas.setFillColor(colors.HexColor("#555555"))
    # Running head
    canvas.drawString(MARGIN_L, PAGE_H - 0.45 * inch,
                      "LineageRank and PipeRCA-Bench")
    canvas.drawRightString(PAGE_W - MARGIN_R, PAGE_H - 0.45 * inch,
                           "Submitted for Review")
    # Page number
    canvas.drawCentredString(PAGE_W / 2, 0.55 * inch,
                             str(canvas.getPageNumber()))
    # Thin rule below header text
    canvas.setStrokeColor(colors.HexColor("#bbbbbb"))
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN_L, PAGE_H - 0.52 * inch,
                PAGE_W - MARGIN_R, PAGE_H - 0.52 * inch)
    canvas.restoreState()


def _add_first_page_footer(canvas, doc):
    _add_header_footer(canvas, doc)


# ── Styles ───────────────────────────────────────────────────────────────────

def build_styles() -> dict:
    base = getSampleStyleSheet()

    def add(name, **kw):
        base.add(ParagraphStyle(name=name, **kw))

    add("PaperTitle",
        fontName="Times-Bold", fontSize=20, leading=24,
        alignment=TA_CENTER, spaceAfter=6, spaceBefore=0)

    add("PaperAuthor",
        fontName="Times-Roman", fontSize=10, leading=13,
        alignment=TA_CENTER, spaceAfter=3)

    add("PaperAffil",
        fontName="Times-Italic", fontSize=9, leading=12,
        alignment=TA_CENTER, spaceAfter=10,
        textColor=colors.HexColor("#333333"))

    add("AbstractLabel",
        fontName="Times-Bold", fontSize=9, leading=11,
        alignment=TA_LEFT, spaceAfter=0)

    add("AbstractBody",
        fontName="Times-Roman", fontSize=9, leading=11.5,
        alignment=TA_JUSTIFY, spaceAfter=6,
        leftIndent=0, rightIndent=0)

    add("IndexTerms",
        fontName="Times-Roman", fontSize=9, leading=11.5,
        alignment=TA_JUSTIFY, spaceAfter=8)

    add("SectionHeading",
        fontName="Times-Bold", fontSize=10, leading=13,
        alignment=TA_CENTER, spaceBefore=10, spaceAfter=4,
        textTransform="uppercase")

    add("SubHeading",
        fontName="Times-BoldItalic", fontSize=10, leading=13,
        alignment=TA_LEFT, spaceBefore=6, spaceAfter=3)

    add("SubSubHeading",
        fontName="Times-Italic", fontSize=10, leading=13,
        alignment=TA_LEFT, spaceBefore=4, spaceAfter=2)

    add("Body",
        fontName="Times-Roman", fontSize=10, leading=13,
        alignment=TA_JUSTIFY, spaceAfter=5,
        firstLineIndent=12)

    add("BodyNoIndent",
        fontName="Times-Roman", fontSize=10, leading=13,
        alignment=TA_JUSTIFY, spaceAfter=5)

    add("BulletBody",
        fontName="Times-Roman", fontSize=10, leading=13,
        alignment=TA_JUSTIFY, spaceAfter=2,
        leftIndent=14, firstLineIndent=-8)

    add("NumberedBody",
        fontName="Times-Roman", fontSize=10, leading=13,
        alignment=TA_JUSTIFY, spaceAfter=2,
        leftIndent=18, firstLineIndent=-12)

    add("FigCaption",
        fontName="Times-Italic", fontSize=8.5, leading=11,
        alignment=TA_CENTER, spaceBefore=4, spaceAfter=8)

    add("TableCaption",
        fontName="Times-Bold", fontSize=9, leading=11,
        alignment=TA_CENTER, spaceBefore=8, spaceAfter=3)

    add("TableCaptionSub",
        fontName="Times-Roman", fontSize=9, leading=11,
        alignment=TA_CENTER, spaceAfter=3)

    add("RefEntry",
        fontName="Times-Roman", fontSize=8.5, leading=11,
        alignment=TA_JUSTIFY, spaceAfter=3,
        leftIndent=14, firstLineIndent=-14)

    add("DefBox",
        fontName="Times-Italic", fontSize=9.5, leading=12,
        alignment=TA_JUSTIFY, spaceAfter=5,
        leftIndent=10, rightIndent=10)

    return base


# ── Inline markup ─────────────────────────────────────────────────────────────

def _ascii_fallback(text: str) -> str:
    # Replace chars outside Times-Roman WinAnsiEncoding with ASCII.
    text = text.replace('−', '-')
    text = text.replace('∈', ' in ')
    text = text.replace('∪', ' u ')
    text = text.replace('≠', '!=')
    text = text.replace('≤', '<=')
    text = text.replace('≥', '>=')
    text = text.replace('α', 'alpha')
    text = text.replace('λ', 'lambda')
    text = text.replace('β', 'beta')
    text = text.replace('γ', 'gamma')
    text = text.replace('δ', 'delta')
    text = text.replace('†', '+')
    text = text.replace('✓', '[OK]')
    text = text.replace('✗', '[X]')
    text = text.replace('──►', '-->')
    text = text.replace('──', '--')
    text = text.replace('─', '-')
    text = text.replace('│', '|')
    text = text.replace('┌', '+')
    text = text.replace('┐', '+')
    text = text.replace('└', '+')
    text = text.replace('┘', '+')
    text = text.replace('┤', '+')
    text = text.replace('├', '+')
    text = text.replace('┼', '+')
    text = text.replace('►', '->')
    text = text.replace('▼', 'v')
    text = text.replace('▲', '^')
    text = text.replace('□', '[ ]')
    text = text.replace('▣', '[*]')
    text = text.replace('■', '[#]')
    return text


def clean_inline(text: str) -> str:
    # Step 1: ASCII fallbacks for chars not in Times-Roman encoding
    text = _ascii_fallback(text)
    # Step 2: WinAnsiEncoding chars -> XML entities
    text = text.replace('—', '&#8212;')
    text = text.replace('–', '&#8211;')
    text = text.replace('…', '&#8230;')
    text = text.replace('’', '&#8217;')
    text = text.replace('‘', '&#8216;')
    text = text.replace('“', '&#8220;')
    text = text.replace('”', '&#8221;')
    text = text.replace('×', '&#215;')
    text = text.replace('§', '&#167;')
    # Step 3: Escape XML special chars
    text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    text = text.replace('&amp;#', '&#')
    # Step 4: Inline markup
    text = re.sub(r'\*\*([^*]+)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'(?<!\*)\*([^*]+)\*(?!\*)', r'<i>\1</i>', text)
    text = re.sub(r'`([^`]+)`', r"<font name='Courier' size='9'>\1</font>", text)
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    return text


# ── Pipe table parser ─────────────────────────────────────────────────────────

def _parse_pipe_table(lines: list[str], styles: dict) -> Table | None:
    """Parse markdown pipe-table rows into a ReportLab Table."""
    rows = []
    for line in lines:
        if not line.strip().startswith("|"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        # Skip markdown separator rows (|---|---|, |:---:|, etc.)
        if cells and all(re.match(r'^[-:]+$', c) for c in cells if c):
            continue
        rows.append(cells)
    if not rows:
        return None

    # Determine column widths based on column count
    ncols = max(len(r) for r in rows)
    # Pad short rows
    rows = [r + [""] * (ncols - len(r)) for r in rows]

    # Convert cell text
    cell_style = ParagraphStyle(
        "TableCell",
        fontName="Times-Roman", fontSize=8.5, leading=11,
        alignment=TA_LEFT, spaceAfter=0,
    )
    header_style = ParagraphStyle(
        "TableHeader",
        fontName="Times-Bold", fontSize=8.5, leading=11,
        alignment=TA_CENTER, spaceAfter=0,
    )

    is_header_row = True
    table_data = []
    for row in rows:
        formatted = []
        for i, cell in enumerate(row):
            s = header_style if is_header_row else cell_style
            formatted.append(Paragraph(clean_inline(cell), s))
        table_data.append(formatted)
        is_header_row = False

    # Column width strategy: equal distribution within column width
    col_w = COL_W - 8  # a little padding
    col_widths = [col_w / ncols] * ncols

    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8e8e8")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTNAME", (0, 0), (-1, 0), "Times-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f4f4f4")]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#aaaaaa")),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#555555")),
    ]))
    return tbl


# ── Markdown → story ──────────────────────────────────────────────────────────

HEADER_H_ESTIMATE = 4.4 * inch   # used to split page-1 frames


def markdown_to_story(text: str):  # noqa: C901
    styles = build_styles()
    story = []

    lines = text.splitlines()
    i = 0
    n = len(lines)

    # State
    in_header_section = True   # True until <!-- BODY --> marker
    in_authors_block = False
    in_abstract_block = False
    para_buf: list[str] = []
    bullet_buf: list[str] = []
    roman_counter = [0]        # section numbering
    sub_counter = [0]          # subsection letter counter
    table_label_buf: list[str] = []   # buffered TABLE caption lines
    table_sub_buf: str = ""

    def flush_para(indent=True):
        if para_buf:
            joined = " ".join(l.strip() for l in para_buf).strip()
            if joined:
                if in_authors_block:
                    # Author lines: bold name → PaperAuthor, italic → PaperAffil
                    if joined.startswith("**") or "<b>" in clean_inline(joined):
                        s = styles["PaperAuthor"]
                    elif joined.startswith("*") or "<i>" in clean_inline(joined):
                        s = styles["PaperAffil"]
                    else:
                        s = styles["PaperAuthor"]
                elif in_abstract_block:
                    s = styles["AbstractBody"]
                else:
                    s = styles["Body"] if indent else styles["BodyNoIndent"]
                story.append(Paragraph(clean_inline(joined), s))
            para_buf.clear()

    def flush_bullets():
        if bullet_buf:
            for item in bullet_buf:
                story.append(Paragraph("• " + clean_inline(item),
                                       styles["BulletBody"]))
            story.append(Spacer(1, 3))
            bullet_buf.clear()

    def next_roman():
        roman_counter[0] += 1
        sub_counter[0] = 0
        nums = ["I","II","III","IV","V","VI","VII","VIII","IX","X",
                "XI","XII","XIII","XIV","XV"]
        return nums[roman_counter[0] - 1] if roman_counter[0] <= len(nums) else str(roman_counter[0])

    def next_sub():
        sub_counter[0] += 1
        return chr(ord("A") + sub_counter[0] - 1)

    while i < n:
        raw = lines[i]
        line = raw.rstrip()
        stripped = line.strip()
        i += 1

        # ── BODY switch marker ───────────────────────────────────────────────
        if stripped == "<!-- BODY -->":
            flush_para(indent=False)
            flush_bullets()
            in_header_section = False
            story.append(HRFlowable(width="100%", thickness=0.5,
                                    color=colors.HexColor("#aaaaaa"),
                                    spaceAfter=6))
            story.append(FrameBreak())  # leave header frame → left column
            continue

        # ── Fenced code block (``` ... ```) ─────────────────────────────────────
        if stripped.startswith("```"):
            flush_para()
            flush_bullets()
            # Collect lines until closing ```
            code_lines = []
            while i < n and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i].rstrip())
                i += 1
            if i < n:
                i += 1  # consume closing ```

            # If this looks like a pipeline topology figure, draw it properly.
            first_content = next((l.strip() for l in code_lines if l.strip()), "")
            if first_content.startswith("Fig.") and "topology" in first_content:
                diag = _draw_pipeline_diagram(COL_W)
                story.append(diag)
                story.append(Spacer(1, 4))
            else:
                # Generic code block: gray box with Courier text
                html_lines = []
                for cl in code_lines:
                    cl_safe = _ascii_fallback(cl)
                    cl_safe = cl_safe.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                    html_lines.append(cl_safe if cl_safe else " ")
                code_para_style = ParagraphStyle(
                    "CodeBox",
                    fontName="Courier", fontSize=5, leading=6.5,
                    alignment=TA_LEFT, spaceAfter=0,
                )
                cell_para = Paragraph("<br/>".join(html_lines), code_para_style)
                fig_tbl = Table([[cell_para]], colWidths=[COL_W - 8])
                fig_tbl.setStyle(TableStyle([
                    ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#f5f5f5")),
                    ("BOX",           (0, 0), (-1, -1), 0.5, colors.HexColor("#aaaaaa")),
                    ("TOPPADDING",    (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LEFTPADDING",   (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
                ]))
                story.append(fig_tbl)
                story.append(Spacer(1, 4))
            continue

        # ── Blank line ───────────────────────────────────────────────────────
        if not stripped:
            flush_para()
            flush_bullets()
            continue

        # ── H1: paper title ──────────────────────────────────────────────────
        if stripped.startswith("# ") and not stripped.startswith("## "):
            flush_para()
            flush_bullets()
            story.append(Spacer(1, 0.1 * inch))
            title = stripped[2:].strip()
            # Split at ":" for visual line break
            parts = title.split(":", 1)
            story.append(Paragraph(clean_inline(parts[0].strip() + ":"),
                                   styles["PaperTitle"]))
            if len(parts) > 1:
                story.append(Paragraph(clean_inline(parts[1].strip()),
                                       styles["PaperAffil"]))
            story.append(Spacer(1, 6))
            continue

        # ── H2: major structural block (Authors, Abstract, section) ──────────
        if stripped.startswith("## ") and not stripped.startswith("### "):
            flush_para()
            flush_bullets()
            label = stripped[3:].strip()

            if in_header_section:
                if label.startswith("Author"):
                    in_authors_block = True
                    in_abstract_block = False
                elif label.startswith("Abstract"):
                    in_authors_block = False
                    in_abstract_block = True
                    story.append(Spacer(1, 4))
                    story.append(HRFlowable(width="100%", thickness=0.8,
                                            color=colors.black, spaceAfter=4))
                    story.append(Paragraph("<b>Abstract</b>—",
                                           styles["AbstractLabel"]))
            else:
                # Body section — References gets no number
                if label.lower() == "references":
                    story.append(Spacer(1, 6))
                    story.append(Paragraph("REFERENCES", styles["SectionHeading"]))
                else:
                    # Detect pre-numbered headings like "I. Introduction"
                    _pre = re.match(r'^([IVXLCDMivxlcdm]+)\.\s+(.+)$', label)
                    if _pre:
                        roman = _pre.group(1).upper()
                        rest = _pre.group(2)
                        sub_counter[0] = 0  # reset subsection counter
                    else:
                        roman = next_roman()
                        rest = label
                    story.append(Spacer(1, 6))
                    story.append(Paragraph(f"{roman}. {rest.upper()}",
                                           styles["SectionHeading"]))
            continue

        # ── H3: subsection ───────────────────────────────────────────────────
        if stripped.startswith("### "):
            flush_para()
            flush_bullets()
            label = stripped[4:].strip()
            if in_header_section:
                pass  # e.g. "### Conservative novelty paragraph"
            else:
                # Detect pre-lettered headings like "A. Provenance and Lineage Systems"
                _presub = re.match(r'^([A-Z])\.\s+(.+)$', label)
                if _presub:
                    letter = _presub.group(1)
                    rest = _presub.group(2)
                else:
                    letter = next_sub()
                    rest = label
                story.append(Paragraph(f"{letter}. {rest}",
                                       styles["SubHeading"]))
            continue

        # ── H4: sub-subsection ───────────────────────────────────────────────
        if stripped.startswith("#### "):
            flush_para()
            flush_bullets()
            story.append(Paragraph(stripped[5:].strip(), styles["SubSubHeading"]))
            continue

        # ── Index Terms line ─────────────────────────────────────────────────
        if stripped.startswith("*Index Terms*") or stripped.startswith("*Index Terms—"):
            flush_para()
            flush_bullets()
            text_part = re.sub(r"^\*Index Terms\*[—–-]*", "", stripped).strip()
            story.append(Spacer(1, 4))
            story.append(Paragraph(
                "<i><b>Index Terms</b></i>—" + clean_inline(text_part),
                styles["IndexTerms"]))
            continue

        # ── TABLE caption — handles "TABLE I" and "**TABLE I**" formats ─────────
        _bare = re.sub(r'\*+', '', stripped).strip()  # strip ** / * markers
        if re.match(r"^TABLE\s+[IVXLCDM]+\b", _bare):
            flush_para()
            flush_bullets()
            # Split "TABLE I — Description" into label and sub-caption
            _parts = re.split(r'\s*[—–]\s*', _bare, maxsplit=1)
            table_label_buf.append(_parts[0].strip())   # e.g. "TABLE I"
            if len(_parts) > 1:
                table_label_buf.append(_parts[1].strip())  # e.g. "Fault-Prior ..."
            continue

        # ── Sub-caption line after TABLE n ────────────────────────────────────
        _bare2 = re.sub(r'\*+', '', stripped).strip()
        if (table_label_buf and not stripped.startswith("|")
                and not re.match(r"^TABLE\s+[IVXLCDM]+\b", _bare2)):
            table_label_buf.append(_bare2)
            continue

        # ── Pipe table rows ───────────────────────────────────────────────────
        if stripped.startswith("|"):
            flush_para()
            flush_bullets()
            # Collect all consecutive pipe lines
            pipe_lines = [stripped]
            while i < n and lines[i].strip().startswith("|"):
                pipe_lines.append(lines[i].strip())
                i += 1

            tbl = _parse_pipe_table(pipe_lines, styles)
            if tbl:
                caption_parts = []
                if table_label_buf:
                    caption_parts.append(
                        Paragraph(table_label_buf[0].upper(),
                                  styles["TableCaption"]))
                    if len(table_label_buf) > 1:
                        caption_parts.append(
                            Paragraph(" ".join(table_label_buf[1:]),
                                      styles["TableCaptionSub"]))
                    table_label_buf.clear()
                story.append(KeepTogether(caption_parts + [tbl, Spacer(1, 6)]))
            continue

        # ── Reference entry [n] ───────────────────────────────────────────────
        if re.match(r"^\[\d+\]", stripped):
            flush_para()
            flush_bullets()
            story.append(Paragraph(clean_inline(stripped), styles["RefEntry"]))
            continue

        # ── Bullet ───────────────────────────────────────────────────────────
        if stripped.startswith("- "):
            flush_para()
            bullet_buf.append(stripped[2:].strip())
            continue

        # ── Horizontal rule ───────────────────────────────────────────────────
        if stripped in {"---", "***", "___"}:
            flush_para()
            flush_bullets()
            story.append(HRFlowable(width="100%", thickness=0.5,
                                    color=colors.HexColor("#aaaaaa"),
                                    spaceAfter=4))
            continue

        # ── Figure embed marker <!-- FIGURE filename caption --> ─────────────
        fig_match = re.match(r"<!--\s*FIGURE\s+(\S+)\s*(.*?)\s*-->", stripped)
        if fig_match:
            flush_para()
            flush_bullets()
            fig_path = ROOT / "exports" / "figures" / fig_match.group(1)
            fig_caption = fig_match.group(2).strip()
            if fig_path.exists():
                from PIL import Image as _PILImg
                _pil = _PILImg.open(str(fig_path))
                _pw, _ph = _pil.size
                _pil.close()
                img_w = COL_W - 4
                img_h = img_w * _ph / _pw
                img = Image(str(fig_path), width=img_w, height=img_h)
                img.hAlign = "CENTER"
                items = [img]
                if fig_caption:
                    items.append(Paragraph(fig_caption, styles["FigCaption"]))
                story.append(KeepTogether(items + [Spacer(1, 4)]))
            continue

        # ── Wide figure marker <!-- WIDEFIG filename caption --> ─────────────
        wide_fig_match = re.match(r"<!--\s*WIDEFIG\s+(\S+)\s*(.*?)\s*-->", stripped)
        if wide_fig_match:
            flush_para()
            flush_bullets()
            fig_path = ROOT / "exports" / "figures" / wide_fig_match.group(1)
            fig_caption = wide_fig_match.group(2).strip()
            if fig_path.exists():
                from PIL import Image as _PILImg
                _pil = _PILImg.open(str(fig_path))
                _pw, _ph = _pil.size
                _pil.close()
                img_w = BODY_W - 8
                img_h = img_w * _ph / _pw
                img = Image(str(fig_path), width=img_w, height=img_h)
                img.hAlign = "CENTER"
                items = [img]
                if fig_caption:
                    items.append(Paragraph(fig_caption, styles["FigCaption"]))
                story.append(KeepTogether(items + [Spacer(1, 6)]))
            continue

        # ── Numbered list items (1. 2. 3. ...) ───────────────────────────────
        numbered_match = re.match(r"^(\d+)\.\s+(.+)$", stripped)
        if numbered_match and not stripped.startswith("##"):
            flush_para()
            flush_bullets()
            num = numbered_match.group(1)
            item_text = numbered_match.group(2)
            story.append(Paragraph(
                f"<b>{num}.</b> {clean_inline(item_text)}",
                styles["NumberedBody"]))
            continue

        # ── Definition / formula box (lines starting with Definition or score_) ──
        if (stripped.startswith("**Definition")
                or re.match(r"^(score_|evidence_|base\(|ev\()", stripped)):
            flush_para()
            flush_bullets()
            story.append(Paragraph(clean_inline(stripped), styles["DefBox"]))
            continue

        # ── Plain paragraph line ──────────────────────────────────────────────
        para_buf.append(stripped)

    flush_para()
    flush_bullets()
    return story


# ── Abstract special rendering ────────────────────────────────────────────────
# The parser above collects abstract text into para_buf and emits it as Body
# paragraphs. We post-process to use AbstractBody style for header-section text.
# Simpler: we track in_abstract state during parsing.
# The current approach outputs AbstractBody naturally because we handle
# ## Abstract → flush, then subsequent lines use para_buf → Body.
# We override Body style inside the header region by patching after the fact.
# For now the abstract text uses Body (10pt); that's readable but not 9pt IEEE.
# A future pass can shrink. Priority: correctness over micro-typography.


def build_doc(output_path: Path):
    HEADER_H = HEADER_H_ESTIMATE
    h_frame, l_frame_p1, r_frame_p1 = _frames_first_page(HEADER_H)
    l_frame, r_frame = _frames_later_page()

    first_pt = PageTemplate(
        id="FirstPage",
        frames=[h_frame, l_frame_p1, r_frame_p1],
        onPage=_add_first_page_footer,
    )
    body_pt = PageTemplate(
        id="TwoCol",
        frames=[l_frame, r_frame],
        onPage=_add_header_footer,
    )

    doc = BaseDocTemplate(
        str(output_path),
        pagesize=LETTER,
        leftMargin=MARGIN_L,
        rightMargin=MARGIN_R,
        topMargin=MARGIN_T,
        bottomMargin=MARGIN_B,
        title="LineageRank and PipeRCA-Bench",
        author="Research",
    )
    doc.addPageTemplates([first_pt, body_pt])
    return doc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path,
                        default=ROOT / "docs" / "lineagerank_revised_draft.md")
    parser.add_argument("--output", type=Path,
                        default=ROOT / "exports" / "lineagerank_revised_draft.pdf")
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    story = markdown_to_story(args.input.read_text())

    # Insert NextPageTemplate after FrameBreak so body pages use TwoCol
    _fb_type = type(FrameBreak)
    for idx, item in enumerate(story):
        if isinstance(item, _fb_type):
            story.insert(idx + 1, NextPageTemplate("TwoCol"))
            break

    doc = build_doc(args.output)
    doc.build(story)
    print(args.output)


if __name__ == "__main__":
    main()
