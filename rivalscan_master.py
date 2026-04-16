"""
RivalScan Master v12 — Complete Business Intelligence Engine
============================================================
v12 FIXES:
  FIX 1 — LinkedIn slug variants (swiggy-in, zomato-in, companyhq etc)
  FIX 2 — App Store API result synced to domain_intel app_ios/android flags
  FIX 3 — Pricing domain validation (no $199 self-price bleed)
  FIX 4 — LinkedIn employee count flags parent-company inflation (>20k)
  FIX 5 — JS-rendered sites flagged in dashboard + AI prompt

SETUP:
    pip install anthropic requests beautifulsoup4 python-dotenv reportlab lxml

USAGE:
    python rivalscan_master_v12.py
"""

import re
import os
import anthropic
from datetime import datetime
from part1_utils_scraping import *
from part2_social_news_seo import *
from part3_apis_intelligence import *

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether
)
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.graphics.shapes import Drawing, Rect, String, Line
from reportlab.graphics import renderPDF

# ── COLORS ──────────────────────────────────────────────────
DARK        = HexColor("#0d0d1a")
ACCENT      = HexColor("#00cc70")
RED         = HexColor("#ff3d6e")
BLUE        = HexColor("#4d9eff")
ORANGE      = HexColor("#ff9500")
PURPLE      = HexColor("#9b59b6")
GREEN       = HexColor("#00aa55")
YELLOW      = HexColor("#f1c40f")
WHITE       = HexColor("#ffffff")
DARKTEXT    = HexColor("#1a1a2e")
LIGHT       = HexColor("#f8f8f8")
BORDER      = HexColor("#dddddd")
MUTED       = HexColor("#777777")
LIGHTGREEN  = HexColor("#e8fff4")
LIGHTRED    = HexColor("#fff0f3")
LIGHTBLUE   = HexColor("#e8f4ff")
LIGHTORANGE = HexColor("#fff8e8")
LIGHTPURPLE = HexColor("#fef0ff")

SECTIONS = {
    "EXECUTIVE SUMMARY":         ("[01]", LIGHTBLUE,    BLUE),
    "URGENT THREATS":            ("[02]", LIGHTRED,     RED),
    "YOUR ADVANTAGES":           ("[03]", LIGHTGREEN,   GREEN),
    "COMPETITOR WEAKNESSES":     ("[04]", LIGHTORANGE,  ORANGE),
    "MARKETING INTELLIGENCE":    ("[05]", LIGHTPURPLE,  PURPLE),
    "TECH STACK INTELLIGENCE":   ("[06]", LIGHTBLUE,    BLUE),
    "HIRING INTELLIGENCE":       ("[07]", LIGHTORANGE,  ORANGE),
    "SOCIAL MEDIA INTELLIGENCE": ("[08]", LIGHTGREEN,   GREEN),
    "NEWS AND PR INTELLIGENCE":  ("[09]", LIGHTGREEN,   GREEN),
    "CONTENT INTELLIGENCE":      ("[10]", LIGHTBLUE,    BLUE),
    "OUTREACH INTELLIGENCE":     ("[11]", LIGHTPURPLE,  PURPLE),
    "30 DAY BATTLE PLAN":        ("[12]", LIGHTBLUE,    BLUE),
}


# ════════════════════════════════════════════════════════════
# BUILD FULL PROFILE
# ════════════════════════════════════════════════════════════

def build_profile(url):
    core   = scrape_website(url)
    domain = core.get("domain","")
    cn     = core.get("company_name","")
    sl     = core.get("social_links",{})

    tech         = detect_tech_stack(url)
    gtm          = scan_gtm_deep(url)
    leadership   = scrape_leadership(url)
    jobs         = scrape_jobs(url, cn)
    blog         = scrape_blog(url)
    facebook     = scrape_facebook(sl.get("facebook",""), cn)
    linkedin     = scrape_linkedin(sl.get("linkedin",""), cn)   # FIX 1 applied here
    twitter      = scrape_twitter(sl.get("twitter","") or sl.get("x",""), cn)
    reviews      = scrape_reviews(cn, domain, url)
    ads          = scan_ad_intelligence(domain, cn)
    fb_ads       = scrape_facebook_ads(cn, domain)
    google_ads   = scrape_google_ads(cn, domain)
    google_intel = google_business_intelligence(cn, domain)
    domain_intel = scan_domain_intelligence(url, domain)
    pricing      = analyze_pricing(url)                          # FIX 3 applied here
    seo          = full_seo_scan(url, cn)

    # Hunter.io emails
    hunter_data = hunter_find_emails(domain)
    if hunter_data and hunter_data["found"]:
        contact = {
            "emails":         [e["email"] for e in hunter_data["emails"]],
            "phones":         [],
            "sales_email":    hunter_data.get("sales_email"),
            "support_email":  hunter_data.get("support_email"),
            "press_email":    hunter_data.get("press_email"),
            "ceo_email":      hunter_data.get("ceo_email"),
            "named_contacts": hunter_data.get("all_named_contacts",[]),
            "organization":   hunter_data.get("organization",""),
            "hunter_verified":True,
        }
        scraped_contact    = scrape_contact(url)
        contact["phones"]  = scraped_contact.get("phones",[])
    else:
        contact                    = scrape_contact(url)
        contact["hunter_verified"] = False
        contact["named_contacts"]  = []

    # PageSpeed
    pagespeed = get_pagespeed(url)
    if pagespeed:
        domain_intel.update({
            "desktop_score":   pagespeed.get("desktop_score"),
            "mobile_score":    pagespeed.get("mobile_score"),
            "lcp":             pagespeed.get("lcp"),
            "cls":             pagespeed.get("cls"),
            "fcp":             pagespeed.get("fcp"),
            "opportunities":   pagespeed.get("opportunities",[]),
        })
        if pagespeed.get("desktop_score"):
            score = pagespeed["desktop_score"]
            domain_intel["page_speed_hint"] = (
                f"Fast (PageSpeed {score}/100)" if score >= 90 else
                f"Good (PageSpeed {score}/100)" if score >= 70 else
                f"Needs work (PageSpeed {score}/100)"
            )

    # YouTube API
    yt_api  = get_youtube_stats(channel_url=sl.get("youtube",""), company_name=cn)
    youtube = yt_api if (yt_api and yt_api["found"]) else scrape_youtube(sl.get("youtube",""), cn)

    # NewsAPI
    news_api = newsapi_search(cn, domain)
    if news_api and news_api["articles"]:
        news = {
            "recent_articles":       news_api["articles"],
            "total_found":           news_api["total"],
            "media_presence":        ("Very High" if news_api["total"]>50 else "High" if news_api["total"]>20 else "Medium" if news_api["total"]>5 else "Low" if news_api["total"]>0 else "None detected"),
            "latest_headline":       news_api["articles"][0]["title"] if news_api["articles"] else None,
            "funding_mentioned":     news_api["funding_mentioned"],
            "acquisition_mentioned": news_api["acquisition_mentioned"],
            "partnership_mentioned": news_api["partnership_mentioned"],
            "award_mentioned":       news_api["award_mentioned"],
            "negative_mentioned":    news_api["negative_mentioned"],
            "signals_summary":       news_api["signals"],
        }
    else:
        news = scrape_news(cn, domain)

    # WHOIS/RDAP
    domain_age = get_domain_age(domain)
    domain_intel.update({
        "domain_age":   domain_age.get("age_years"),
        "registered":   domain_age.get("registered"),
        "registrar":    domain_age.get("registrar"),
        "trust_signal": domain_age.get("trust_signal"),
        "nameservers":  domain_age.get("nameservers",[]),
    })

    # New intelligence modules
    app_store    = scrape_app_store(cn, domain)
    reddit       = scrape_reddit_sentiment(cn, domain)
    pricing_deep = deep_pricing_intelligence(url, cn)           # FIX 3 applied here
    geo          = analyze_geographic_expansion(url, cn, domain)
    product      = analyze_product_features(url, cn, domain)

    # FIX 2: sync App Store API result → domain_intel flags
    if app_store.get("ios_found"):
        domain_intel["app_ios"]     = True
    if app_store.get("android_found"):
        domain_intel["app_android"] = True

    return dict(
        core=core, tech=tech, gtm=gtm, leadership=leadership,
        contact=contact, jobs=jobs, blog=blog,
        facebook=facebook, linkedin=linkedin,
        youtube=youtube, twitter=twitter, news=news,
        seo=seo, reviews=reviews, ads=ads,
        domain_intel=domain_intel, pricing=pricing,
        fb_ads=fb_ads, google_ads=google_ads,
        google_intel=google_intel,
        app_store=app_store, reddit=reddit,
        pricing_deep=pricing_deep, geo=geo, product=product,
    )


# ════════════════════════════════════════════════════════════
# AI MASTER ANALYSIS
# ════════════════════════════════════════════════════════════

def generate_master_report(my, comp):
    print(f"\n  🧠 AI Master Analysis...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def fmt(p):
        c   = p["core"]
        li  = p["linkedin"]
        j   = p["jobs"]
        b   = p["blog"]
        fb  = p["facebook"]
        yt  = p["youtube"]
        tw  = p["twitter"]
        n   = p["news"]
        t   = p["tech"]

        # FIX 4 + FIX 5: enrich with data quality notes
        employee_str = li.get("employees","?") or "?"
        if li.get("employees_note"):
            employee_str += f" {li['employees_note']}"
        js_warning = "[WARNING: JS-rendered site — data gaps expected. Treat Swiggy-side metrics with lower confidence]" if c.get("js_rendered") else ""

        return f"""
Company: {c.get('company_name')} | URL: {c.get('url')} | Country: {c.get('country','?')}
{js_warning}
Value Prop: {c.get('value_proposition')}
Meta: {c.get('meta_description')}
Nav: {', '.join(c.get('navigation_items',[])[:8])}
Content: {c.get('main_content','')[:600]}

TECH: {len(t.get('all',[]))} tools | {', '.join(t.get('all',[])[:10]) or 'None'}
  Analytics: {', '.join(t.get('analytics',[])) or 'None'}
  Marketing: {', '.join(t.get('marketing',[])) or 'None'}
  Ads: {', '.join(t.get('ads',[])) or 'None'}

LEADERSHIP: CEO: {p['leadership'].get('ceo','Not public')} | Team: {p['leadership'].get('team_size_hint')}

CONTACT: Sales: {p['contact'].get('sales_email','None')} | CEO: {p['contact'].get('ceo_email','None')} | Named: {len(p['contact'].get('named_contacts',[]))} contacts

HIRING: {j.get('hiring_signal')} | Depts: {', '.join(j.get('departments',{}).keys())}
  Roles: {', '.join(j.get('open_positions',[])[:5])}

BLOG: {b.get('posting_frequency')} | {b.get('total_posts_found',0)} posts | Topics: {', '.join(b.get('content_topics',[])[:6])}
  Recent: {'; '.join([post['title'] for post in b.get('recent_posts',[])[:4]])}

SOCIAL:
  LinkedIn: {'Found' if li.get('found') else 'Not found'} | Followers: {li.get('followers','?')} | Employees: {employee_str}
  YouTube: {'Found' if yt.get('found') else 'Not found'} | Subs: {yt.get('subscribers','?')}
  Facebook: {'Found' if fb.get('found') else 'Not found'} | Twitter: {'Found' if tw.get('found') else 'Not found'}

NEWS: {n.get('media_presence')} | {n.get('total_found',0)} articles | Signals: {', '.join(n.get('signals_summary',[])) or 'None'}
  Latest: {n.get('latest_headline','None')}

SEO: {p['seo']['onpage'].get('score',0)}/100 Grade {p['seo']['onpage'].get('grade','?')}
  Sitemap: {p['seo']['sitemap'].get('total_urls',0)} pages | Google indexed: {p['seo']['keywords'].get('google_indexed_pages','Unknown')}
  Landing pages: {len(p['seo']['keywords'].get('landing_pages_found',[]))} | Subdomains: {len(p['seo']['keywords'].get('subdomains_found',[]))}
  Keywords: {', '.join(p['seo']['keywords'].get('top_keywords_from_content',[])[:8])}

GTM: {len(p['gtm'].get('hidden_tools',[]))} hidden tools: {', '.join(p['gtm'].get('hidden_tools',[])[:8]) or 'None'}

ADS:
  Facebook: {'ACTIVE - ' + str(p['fb_ads'].get('total_ads',0)) + ' ads | Themes: ' + ', '.join(p['fb_ads'].get('messaging_angles',[])[:3]) if p['fb_ads'].get('found') else 'Not detected'}
  Google: {'Active' if p['google_ads'].get('found') else 'Not detected'}
  FB Spend tier: {p['fb_ads'].get('spend_estimate','Unknown')}
  Google Maps: {'Listed' if p['google_intel'].get('google_maps') else 'Not found'} | Knowledge Panel: {'Yes' if p['google_intel'].get('knowledge_panel') else 'No'}

DOMAIN: Speed: {p['domain_intel'].get('page_speed_hint','?')} | Desktop: {p['domain_intel'].get('desktop_score','?')}/100 | Mobile: {p['domain_intel'].get('mobile_score','?')}/100
  Age: {p['domain_intel'].get('domain_age','?')} years | iOS: {p['domain_intel'].get('app_ios',False)} | Android: {p['domain_intel'].get('app_android',False)} | WhatsApp: {p['domain_intel'].get('whatsapp_business',False)}

PRICING: {p['pricing_deep'].get('pricing_model','Unknown')}
  Subscription: {p['pricing_deep'].get('has_subscription',False)} | Plans: {', '.join(p['pricing_deep'].get('subscription_plans',[])[:3]) or 'None'}
  Price points: {', '.join(p['pricing_deep'].get('price_points',[])[:5]) or 'Not public'}
  Commission: {p['pricing_deep'].get('commission_rate','Unknown')} | Free tier: {p['pricing_deep'].get('free_tier',False)}
  Delivery fees: {', '.join(p['pricing_deep'].get('delivery_fees',[])[:2]) or 'Not public'}
  Membership benefits: {'; '.join(p['pricing_deep'].get('membership_benefits',[])[:3]) or 'None'}

APP: iOS: {p['app_store'].get('ios_rating','?')}/5 ({p['app_store'].get('ios_reviews','?')} ratings) | Android installs: {p['app_store'].get('android_installs','?')}
  Sentiment: {p['app_store'].get('overall_sentiment','Unknown')} | Complaints: {', '.join(p['app_store'].get('top_complaints',[])[:5])} | Praises: {', '.join(p['app_store'].get('top_praises',[])[:5])}

REDDIT: {p['reddit'].get('total_mentions',0)} mentions | Sentiment: {p['reddit'].get('sentiment_score',0):+}%
  Topics: {', '.join(p['reddit'].get('trending_topics',[])[:6]) or 'None'}

GEO: {p['geo'].get('total_locations',0)} cities | {p['geo'].get('geographic_spread','?')} | Markets: {', '.join(p['geo'].get('countries_detected',[])[:4]) or 'Unknown'}
  Cities: {', '.join(p['geo'].get('cities_detected',[])[:8]) or 'Not detected'}

PRODUCT: {', '.join(p['product'].get('feature_categories',[])[:8]) or 'None'} | Tech: {', '.join(p['product'].get('tech_signals',[])[:5]) or 'None'}
"""

    prompt_part1 = f"""
You are the world's best competitive intelligence analyst.
Produce a brutally honest intelligence report. PLAIN TEXT ONLY. No markdown, no ** or ## symbols.
Use these EXACT section headers in ALL CAPS on their own line:

EXECUTIVE SUMMARY
URGENT THREATS
YOUR ADVANTAGES
COMPETITOR WEAKNESSES
MARKETING INTELLIGENCE
TECH STACK INTELLIGENCE

CRITICAL INSTRUCTION: When a company's data shows a JS-rendered site warning, acknowledge data limitations
rather than making strong conclusions based on potentially missing data.
CRITICAL INSTRUCTION: LinkedIn employee counts marked "(may include subsidiaries)" should be noted as approximate.
Every insight must cite actual data. No generic advice — only specific, actionable intelligence.

{'='*50}
MY COMPANY
{'='*50}
{fmt(my)}

{'='*50}
COMPETITOR
{'='*50}
{fmt(comp)}
"""

    prompt_part2 = f"""
You are an elite competitive intelligence analyst. Continue the report.

MY COMPANY: {my["core"].get("company_name")} ({my["core"].get("url")})
  JS-rendered site: {my["core"].get("js_rendered",False)} — {"treat Swiggy data with lower confidence" if my["core"].get("js_rendered") else "full data available"}

COMPETITOR: {comp["core"].get("company_name")} ({comp["core"].get("url")})
  JS-rendered site: {comp["core"].get("js_rendered",False)}

KEY DATA POINTS:
  My LinkedIn followers: {my["linkedin"].get("followers","?")} {my["linkedin"].get("employees_note","") or ""}
  Comp LinkedIn followers: {comp["linkedin"].get("followers","?")} {comp["linkedin"].get("employees_note","") or ""}
  My YouTube subs: {my["youtube"].get("subscribers","?")} | Comp YouTube subs: {comp["youtube"].get("subscribers","?")}
  My Reddit sentiment: {my["reddit"].get("sentiment_score",0):+}% | Comp Reddit: {comp["reddit"].get("sentiment_score",0):+}%
  My app sentiment: {my["app_store"].get("overall_sentiment","Unknown")} | Comp app: {comp["app_store"].get("overall_sentiment","Unknown")}
  My SEO: {my["seo"]["onpage"].get("score",0)}/100 | Comp SEO: {comp["seo"]["onpage"].get("score",0)}/100
  My hiring: {my["jobs"].get("hiring_signal")} | Comp hiring: {comp["jobs"].get("hiring_signal")}
  My news: {my["news"].get("media_presence")} | Comp news: {comp["news"].get("media_presence")}
  My pricing: {my["pricing_deep"].get("pricing_model","Unknown")} | Comp pricing: {comp["pricing_deep"].get("pricing_model","Unknown")}
  My geo: {my["geo"].get("geographic_spread","?")} | Comp geo: {comp["geo"].get("geographic_spread","?")}

Use PLAIN TEXT ONLY. No markdown. These EXACT section headers in ALL CAPS on their own line:

HIRING INTELLIGENCE
SOCIAL MEDIA INTELLIGENCE
NEWS AND PR INTELLIGENCE
CONTENT INTELLIGENCE
OUTREACH INTELLIGENCE
30 DAY BATTLE PLAN

For 30 DAY BATTLE PLAN:
Week 1 - Do Today: [3 specific actions with real data cited]
Week 2 - Do This Week: [3 specific actions]
Week 3 - Build: [3 specific actions]
Week 4 - Scale: [3 specific actions]

Be brutally specific. Use actual data from the intelligence above. No generic advice.
"""

    def clean_text(text):
        lines = []
        for line in text.split("\n"):
            if len(line) > 500:
                lines.extend(re.split(r"(?<=[.!?])\s+", line))
            else:
                lines.append(line)
        return "\n".join(lines)

    r1 = client.messages.create(model="claude-sonnet-4-6", max_tokens=3000, messages=[{"role":"user","content":prompt_part1}])
    r2 = client.messages.create(model="claude-sonnet-4-6", max_tokens=2500, messages=[{"role":"user","content":prompt_part2}])
    return clean_text(r1.content[0].text) + "\n\n" + clean_text(r2.content[0].text)


# ════════════════════════════════════════════════════════════
# CHART BUILDERS
# ════════════════════════════════════════════════════════════

def make_score_bar_chart(scores_my, scores_comp, labels, width=500, height=200):
    drawing = Drawing(width, height)
    bar_w, gap, group_w = 18, 6, 44
    x_start, max_val, chart_h, y_base = 60, 10, 140, 30
    for i, (label, my_s, comp_s) in enumerate(zip(labels, scores_my, scores_comp)):
        x    = x_start + i * group_w
        my_h = int((my_s / max_val) * chart_h)
        drawing.add(Rect(x, y_base, bar_w, my_h, fillColor=HexColor("#00aa55"), strokeColor=None))
        drawing.add(String(x+bar_w//2, y_base+my_h+3, str(my_s), fontSize=8, fillColor=HexColor("#00aa55"), textAnchor="middle"))
        comp_h = int((comp_s / max_val) * chart_h)
        drawing.add(Rect(x+bar_w+gap, y_base, bar_w, comp_h, fillColor=HexColor("#ff3d6e"), strokeColor=None))
        drawing.add(String(x+bar_w+gap+bar_w//2, y_base+comp_h+3, str(comp_s), fontSize=8, fillColor=HexColor("#ff3d6e"), textAnchor="middle"))
        short = label[:10]+".." if len(label)>10 else label
        drawing.add(String(x+bar_w+gap//2, y_base-12, short, fontSize=7, fillColor=HexColor("#555555"), textAnchor="middle"))
    drawing.add(Line(x_start-5, y_base, x_start+len(labels)*group_w, y_base, strokeColor=HexColor("#dddddd"), strokeWidth=1))
    drawing.add(Rect(width-120, height-20, 12, 12, fillColor=HexColor("#00aa55"), strokeColor=None))
    drawing.add(String(width-105, height-20+2, "Your Business", fontSize=8, fillColor=HexColor("#333333")))
    drawing.add(Rect(width-120, height-38, 12, 12, fillColor=HexColor("#ff3d6e"), strokeColor=None))
    drawing.add(String(width-105, height-38+2, "Competitor", fontSize=8, fillColor=HexColor("#333333")))
    return drawing

def make_tech_stack_visual(tech, width=490, height=80):
    drawing = Drawing(width, height)
    tools   = tech.get("all",[])
    if not tools:
        drawing.add(String(width//2, height//2, "No tools detected", fontSize=10, fillColor=HexColor("#aaaaaa"), textAnchor="middle"))
        return drawing
    cols    = 5
    cell_w  = width // cols
    cell_h  = 22
    cat_colors = {"analytics":HexColor("#4d9eff"),"marketing":HexColor("#9b59b6"),"ads":HexColor("#ff9500"),"payments":HexColor("#00aa55"),"support":HexColor("#ff3d6e"),"cms":HexColor("#f1c40f"),"hosting":HexColor("#555555")}
    def get_cat(tool):
        for cat, items in tech.items():
            if cat != "all" and tool in items: return cat
        return "other"
    for i, tool in enumerate(tools[:20]):
        col   = i % cols; row = i // cols
        x     = col * cell_w + 4; y = height - (row+1)*cell_h - 4
        color = cat_colors.get(get_cat(tool), HexColor("#888888"))
        drawing.add(Rect(x, y, cell_w-8, cell_h-4, fillColor=color, strokeColor=None, rx=3, ry=3))
        label = tool[:14] if len(tool)>14 else tool
        drawing.add(String(x+(cell_w-8)//2, y+5, label, fontSize=7, fillColor=WHITE, textAnchor="middle"))
    return drawing


# ════════════════════════════════════════════════════════════
# PDF BUILDER (condensed, clean version)
# ════════════════════════════════════════════════════════════

def build_master_pdf(my_url, comp_url, my_profile, comp_profile, report_text, filename):
    print(f"\n  📄 Building PDF...")

    class NumberedCanvas(rl_canvas.Canvas):
        def __init__(self, *args, **kwargs):
            rl_canvas.Canvas.__init__(self, *args, **kwargs)
            self._saved_page_states = []
        def showPage(self):
            self._saved_page_states.append(dict(self.__dict__))
            self._startPage()
        def save(self):
            num_pages = len(self._saved_page_states)
            for state in self._saved_page_states:
                self.__dict__.update(state)
                self.draw_page_number(num_pages)
                rl_canvas.Canvas.showPage(self)
            rl_canvas.Canvas.save(self)
        def draw_page_number(self, page_count):
            self.setStrokeColor(HexColor("#dddddd"))
            self.setLineWidth(0.5)
            self.line(1.8*cm, 1.2*cm, A4[0]-1.8*cm, 1.2*cm)
            self.setFont("Helvetica", 7)
            self.setFillColor(HexColor("#888888"))
            self.drawRightString(A4[0]-1.8*cm, 0.8*cm, f"RivalScan.ai — Confidential — Page {self._pageNumber} of {page_count}")
            self.drawString(1.8*cm, 0.8*cm, f"Generated: {datetime.now().strftime('%B %d, %Y')}")

    doc      = SimpleDocTemplate(filename, pagesize=A4, rightMargin=1.8*cm, leftMargin=1.8*cm, topMargin=1.8*cm, bottomMargin=2.2*cm)
    styles   = getSampleStyleSheet()
    elements = []

    my_name   = clean_name_for_display(my_profile["core"].get("company_name","My Company"), my_url)
    comp_name = clean_name_for_display(comp_profile["core"].get("company_name","Competitor"), comp_url)

    def S(n, **k):
        return ParagraphStyle(n, parent=styles["Normal"], **k)

    title_s = S("T",  fontSize=22, fontName="Helvetica-Bold", textColor=WHITE,    alignment=TA_CENTER)
    sub_s   = S("Su", fontSize=10, fontName="Helvetica",      textColor=HexColor("#aaaaaa"), alignment=TA_CENTER)
    h2_s    = S("H2", fontSize=11, fontName="Helvetica-Bold", textColor=DARKTEXT, spaceAfter=8, spaceBefore=4)
    body_s  = S("B",  fontSize=9,  fontName="Helvetica",      textColor=DARKTEXT, spaceAfter=4, leading=14)
    bold_s  = S("Bo", fontSize=9,  fontName="Helvetica-Bold", textColor=DARKTEXT, spaceAfter=3)
    small_s = S("Sm", fontSize=8,  fontName="Helvetica",      textColor=DARKTEXT, spaceAfter=2, leading=12)
    num_s   = S("N",  fontSize=9,  fontName="Helvetica-Bold", textColor=BLUE,     spaceAfter=2, spaceBefore=6)
    label_s = S("La", fontSize=8,  fontName="Helvetica-Bold", textColor=HexColor("#555555"), alignment=TA_CENTER)
    footer_s= S("F",  fontSize=7,  fontName="Helvetica",      textColor=HexColor("#aaaaaa"), alignment=TA_CENTER)

    def dark_box(p, pt=14, pb=14):
        t = Table([[p]], colWidths=[17.4*cm])
        t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),DARK),("TOPPADDING",(0,0),(-1,-1),pt),("BOTTOMPADDING",(0,0),(-1,-1),pb),("LEFTPADDING",(0,0),(-1,-1),14),("RIGHTPADDING",(0,0),(-1,-1),14)]))
        return t

    def sec_hdr(icon, title, accent):
        p = Paragraph(f"{icon}  {title}", S("SH", fontSize=12, fontName="Helvetica-Bold", textColor=WHITE))
        t = Table([[p]], colWidths=[17.4*cm])
        t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),DARK),("TOPPADDING",(0,0),(-1,-1),9),("BOTTOMPADDING",(0,0),(-1,-1),9),("LEFTPADDING",(0,0),(-1,-1),12),("LINEBELOW",(0,0),(-1,-1),3,accent)]))
        return t

    def stat_row(label, v1, v2):
        return [
            Paragraph(label, S("SRL", fontSize=8, fontName="Helvetica-Bold", textColor=HexColor("#555555"))),
            Paragraph(str(v1), S("SRV", fontSize=8, fontName="Helvetica", textColor=DARKTEXT, alignment=TA_CENTER)),
            Paragraph(str(v2), S("SRC", fontSize=8, fontName="Helvetica", textColor=DARKTEXT, alignment=TA_CENTER)),
        ]

    def make_stats_table(rows):
        t = Table(rows, colWidths=[5.4*cm, 6*cm, 6*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),DARK),("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE,LIGHT]),
            ("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),
            ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
            ("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(1,0),(-1,-1),"CENTER"),
        ]))
        return t

    def two_col(left, right, bg_left=LIGHTGREEN, bg_right=LIGHTRED):
        t = Table([[left, right]], colWidths=[8.7*cm, 8.7*cm], splitByRow=True)
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(0,-1),bg_left),("BACKGROUND",(1,0),(1,-1),bg_right),
            ("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),
            ("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),
            ("LEFTPADDING",(0,0),(-1,-1),8),("VALIGN",(0,0),(-1,-1),"TOP"),
        ]))
        return t

    # ── COVER ─────────────────────────────────────────────────
    elements.append(dark_box(Paragraph("RIVALSCAN.AI", title_s), pt=20, pb=8))
    elements.append(Spacer(1, 0.2*cm))
    elements.append(Paragraph("Master Business Intelligence Report", sub_s))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}", S("M", fontSize=8, fontName="Helvetica", textColor=HexColor("#888888"), alignment=TA_CENTER)))
    elements.append(Spacer(1, 0.4*cm))

    url_data = [
        [Paragraph("YOUR BUSINESS", S("YL",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR", S("CL",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        [Paragraph(my_name,   S("YN",fontSize=13,fontName="Helvetica-Bold",textColor=DARKTEXT,alignment=TA_CENTER)), Paragraph(comp_name, S("CN",fontSize=13,fontName="Helvetica-Bold",textColor=DARKTEXT,alignment=TA_CENTER))],
        [Paragraph(my_url,    S("YU",fontSize=8,fontName="Helvetica",textColor=MUTED,alignment=TA_CENTER)),           Paragraph(comp_url, S("CU",fontSize=8,fontName="Helvetica",textColor=MUTED,alignment=TA_CENTER))],
        [Paragraph(my_profile["core"].get("country","Global") or "Global", S("YC",fontSize=8,fontName="Helvetica",textColor=MUTED,alignment=TA_CENTER)), Paragraph(comp_profile["core"].get("country","Global") or "Global", S("CC",fontSize=8,fontName="Helvetica",textColor=MUTED,alignment=TA_CENTER))],
    ]
    # FIX 5: show JS-rendered warning on cover
    if my_profile["core"].get("js_rendered"):
        url_data.append([Paragraph("⚠ JS-rendered — limited data", S("JW",fontSize=8,fontName="Helvetica-Bold",textColor=ORANGE,alignment=TA_CENTER)), Paragraph("", body_s)])
    if comp_profile["core"].get("js_rendered"):
        url_data.append([Paragraph("", body_s), Paragraph("⚠ JS-rendered — limited data", S("JW2",fontSize=8,fontName="Helvetica-Bold",textColor=ORANGE,alignment=TA_CENTER))])

    ut = Table(url_data, colWidths=[8.7*cm, 8.7*cm])
    ut.setStyle(TableStyle([("BACKGROUND",(0,0),(0,-1),LIGHTGREEN),("BACKGROUND",(1,0),(1,-1),LIGHTRED),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,-1),"CENTER")]))
    elements.append(ut)
    elements.append(Spacer(1, 0.4*cm))

    disclaimer = "CONFIDENTIAL — This report has been generated by RivalScan.ai using publicly available data. All information is sourced from public websites, news sources, job listings, and social media. No proprietary or private data has been accessed. This report is intended for strategic planning purposes only. RivalScan.ai makes no guarantees regarding accuracy. Recipients should independently verify key findings before making business decisions."
    elements.append(Paragraph(disclaimer, S("DIS", fontSize=7, fontName="Helvetica", textColor=HexColor("#999999"), leading=11, spaceAfter=6)))
    elements.append(PageBreak())

    # ── TABLE OF CONTENTS ─────────────────────────────────────
    elements.append(dark_box(Paragraph("TABLE OF CONTENTS", S("TOC_T", fontSize=16, fontName="Helvetica-Bold", textColor=WHITE, alignment=TA_CENTER))))
    elements.append(Spacer(1, 0.4*cm))
    toc_items = [
        ("1.","Executive Intelligence Dashboard","Data tables + key metrics"),
        ("2.","Competitive Score Charts","Visual comparison graphs"),
        ("3.","Tech Stack Analysis","Tools detected on both sites"),
        ("4.","Hidden Tech Stack (GTM)","Tools inside Google Tag Manager"),
        ("5.","Reviews & Reputation","Star ratings and review signals"),
        ("6.","Ads, Pricing & Digital Presence","Ad spend, pricing, apps"),
        ("7.","Hiring Intelligence","Job openings by department"),
        ("8.","Blog & Content Comparison","Content strategy analysis"),
        ("9.","News & PR Comparison","Recent media coverage"),
        ("10.","SEO Intelligence","SEO scores, indexed pages, landing pages"),
        ("11.","Contact Intelligence","Emails, phones, key contacts"),
        ("12.","Executive Summary","AI-generated overview"),
        ("13.","Urgent Threats","Critical risks requiring action"),
        ("14.","Your Advantages","Competitive strengths"),
        ("15.","Competitor Weaknesses","Exploitable gaps"),
        ("16.","Marketing Intelligence","Marketing strategy analysis"),
        ("17.","Tech Stack Intelligence","Technology strategy insights"),
        ("18.","Hiring Intelligence","6-month roadmap prediction"),
        ("19.","Social Media Intelligence","Platform-by-platform analysis"),
        ("20.","News & PR Intelligence","Media strategy insights"),
        ("21.","30-Day Battle Plan","Week-by-week action plan"),
        ("22.","Ad Intelligence","Facebook + Google ads analysis"),
        ("23.","Google Business","Maps, ratings, knowledge panel"),
        ("24.","App Store","iOS/Android ratings, reviews, sentiment"),
        ("25.","Reddit Sentiment","Community mentions and brand perception"),
        ("26.","Pricing Intelligence","Subscription plans, fees, offers"),
        ("27.","Geographic Footprint","Cities, expansion, market coverage"),
        ("28.","Product Intelligence","Features, changelog, tech signals"),
    ]
    toc_data = []
    for num, title, desc in toc_items:
        toc_data.append([Paragraph(num, S("TN",fontSize=9,fontName="Helvetica-Bold",textColor=BLUE)), Paragraph(title, S("TT",fontSize=9,fontName="Helvetica-Bold",textColor=DARKTEXT)), Paragraph(desc, S("TD",fontSize=8,fontName="Helvetica",textColor=MUTED))])
    toc_t = Table(toc_data, colWidths=[1.2*cm, 8*cm, 8.2*cm])
    toc_t.setStyle(TableStyle([("ROWBACKGROUNDS",(0,0),(-1,-1),[WHITE,HexColor("#f8f8f8")]),("BOX",(0,0),(-1,-1),0.5,BORDER),("INNERGRID",(0,0),(-1,-1),0.3,BORDER),("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6),("LEFTPADDING",(0,0),(-1,-1),8),("VALIGN",(0,0),(-1,-1),"MIDDLE")]))
    elements.append(toc_t)
    elements.append(PageBreak())

    # ── EXECUTIVE DASHBOARD ───────────────────────────────────
    mc  = my_profile["core"];   cc  = comp_profile["core"]
    mli = my_profile["linkedin"]; cli = comp_profile["linkedin"]
    myt = my_profile["youtube"]; cyt = comp_profile["youtube"]
    mn  = my_profile["news"];   cn  = comp_profile["news"]
    mfb = my_profile["facebook"]; cfb = comp_profile["facebook"]
    mtw = my_profile["twitter"]; ctw = comp_profile["twitter"]
    mj  = my_profile["jobs"];   cj  = comp_profile["jobs"]
    mb  = my_profile["blog"];   cbb = comp_profile["blog"]
    ml  = my_profile["leadership"]; cl = comp_profile["leadership"]
    mct = my_profile["contact"]; cct = comp_profile["contact"]

    # FIX 4: employee count with note
    my_emp_str   = (mli.get("employees","—") or "—") + (f" {mli.get('employees_note','')}" if mli.get("employees_note") else "")
    comp_emp_str = (cli.get("employees","—") or "—") + (f" {cli.get('employees_note','')}" if cli.get("employees_note") else "")
    # FIX 5: data quality row
    my_dq   = "⚠ JS-rendered — limited" if mc.get("js_rendered") else "Full scrape"
    comp_dq = "⚠ JS-rendered — limited" if cc.get("js_rendered") else "Full scrape"

    exec_stats = [
        [Paragraph("METRIC",label_s), Paragraph("YOUR BUSINESS",S("YH",fontSize=8,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR",S("CH",fontSize=8,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        stat_row("Data Quality",       my_dq,    comp_dq),   # FIX 5
        stat_row("CEO / Founder",      ml.get("ceo","Not public") or "Not public",    cl.get("ceo","Not public") or "Not public"),
        stat_row("Team Size",          ml.get("team_size_hint","?"),                  cl.get("team_size_hint","?")),
        stat_row("Contact Email",      (mct.get("sales_email","Not found") or "Not found")[:40]+(" [V]" if mct.get("hunter_verified") else ""),  (cct.get("sales_email","Not found") or "Not found")[:40]+(" [V]" if cct.get("hunter_verified") else "")),
        stat_row("Tech Tools",         len(my_profile["tech"].get("all",[])),         len(comp_profile["tech"].get("all",[]))),
        stat_row("Blog Posts",         mb.get("total_posts_found",0),                 cbb.get("total_posts_found",0)),
        stat_row("Blog Frequency",     mb.get("posting_frequency","None"),             cbb.get("posting_frequency","None")),
        stat_row("LinkedIn Followers", mli.get("followers","—"),                       cli.get("followers","—")),
        stat_row("LinkedIn Employees", my_emp_str,                                     comp_emp_str),  # FIX 4
        stat_row("YouTube",            f"{'Active' if myt.get('found') else 'Not found'} {myt.get('subscribers','') or ''}", f"{'Active' if cyt.get('found') else 'Not found'} {cyt.get('subscribers','') or ''}"),
        stat_row("Facebook",           f"{'Active' if mfb.get('found') else 'Not found'}",  f"{'Active' if cfb.get('found') else 'Not found'}"),
        stat_row("Twitter/X",          mtw.get("followers","—"),                        ctw.get("followers","—")),
        stat_row("News Articles",      f"{mn.get('total_found',0)} ({mn.get('media_presence','?')})", f"{cn.get('total_found',0)} ({cn.get('media_presence','?')})"),
        stat_row("Open Job Roles",     mj.get("total_jobs",0),                         cj.get("total_jobs",0)),
        stat_row("Funding News",       "YES - Alert!" if mn.get("funding_mentioned") else "No", "YES - Alert!" if cn.get("funding_mentioned") else "No"),
        stat_row("SEO Score",          f"{my_profile['seo']['onpage']['score']}/100 Grade {my_profile['seo']['onpage']['grade']}", f"{comp_profile['seo']['onpage']['score']}/100 Grade {comp_profile['seo']['onpage']['grade']}"),
        stat_row("Pages in Sitemap",   my_profile["seo"]["sitemap"]["total_urls"],     comp_profile["seo"]["sitemap"]["total_urls"]),
        stat_row("Google Indexed",     my_profile["seo"]["keywords"].get("google_indexed_pages","—") or "—", comp_profile["seo"]["keywords"].get("google_indexed_pages","—") or "—"),
        stat_row("Landing Pages",      f"{len(my_profile['seo']['keywords'].get('landing_pages_found',[]))} pages", f"{len(comp_profile['seo']['keywords'].get('landing_pages_found',[]))} pages"),
        stat_row("Subdomains Active",  f"{len(my_profile['seo']['keywords'].get('subdomains_found',[]))} found", f"{len(comp_profile['seo']['keywords'].get('subdomains_found',[]))} found"),
        stat_row("GTM Hidden Tools",   len(my_profile.get("gtm",{}).get("hidden_tools",[])),  len(comp_profile.get("gtm",{}).get("hidden_tools",[]))),
        stat_row("Site Review Rating", f"{my_profile.get('reviews',{}).get('site_rating','—')}/5",  f"{comp_profile.get('reviews',{}).get('site_rating','—')}/5"),
        stat_row("Google Ads",         "Active" if my_profile.get("ads",{}).get("google_ads_detected") else "Not detected", "Active" if comp_profile.get("ads",{}).get("google_ads_detected") else "Not detected"),
        stat_row("Facebook Ads",
            f"Active ({my_profile.get('fb_ads',{}).get('total_ads',0)} ads)" if my_profile.get("fb_ads",{}).get("found") else "Not detected",
            f"Active ({comp_profile.get('fb_ads',{}).get('total_ads',0)} ads)" if comp_profile.get("fb_ads",{}).get("found") else "Not detected"),
        stat_row("FB Ad Spend Tier",   my_profile.get("fb_ads",{}).get("spend_estimate","—"),  comp_profile.get("fb_ads",{}).get("spend_estimate","—")),
        stat_row("Google Business",    my_profile.get("google_intel",{}).get("google_rating","—") or "—",  comp_profile.get("google_intel",{}).get("google_rating","—") or "—"),
        stat_row("Google Maps",        "Listed" if my_profile.get("google_intel",{}).get("google_maps") else "Not found", "Listed" if comp_profile.get("google_intel",{}).get("google_maps") else "Not found"),
        stat_row("Page Speed",         my_profile.get("domain_intel",{}).get("page_speed_hint","—"), comp_profile.get("domain_intel",{}).get("page_speed_hint","—")),
        stat_row("iOS App",            "Yes" if my_profile.get("domain_intel",{}).get("app_ios") else "No", "Yes" if comp_profile.get("domain_intel",{}).get("app_ios") else "No"),  # FIX 2
        stat_row("Android App",        "Yes" if my_profile.get("domain_intel",{}).get("app_android") else "No", "Yes" if comp_profile.get("domain_intel",{}).get("app_android") else "No"),  # FIX 2
        stat_row("WhatsApp Business",  "Yes" if my_profile.get("domain_intel",{}).get("whatsapp_business") else "No", "Yes" if comp_profile.get("domain_intel",{}).get("whatsapp_business") else "No"),
        stat_row("Pricing Strategy",   my_profile.get("pricing",{}).get("pricing_strategy","—"), comp_profile.get("pricing",{}).get("pricing_strategy","—")),
        stat_row("iOS App Rating",
            f"{my_profile.get('app_store',{}).get('ios_rating','—')}/5 ({my_profile.get('app_store',{}).get('ios_reviews','—')} ratings)" if my_profile.get("app_store",{}).get("ios_found") else "Not found",
            f"{comp_profile.get('app_store',{}).get('ios_rating','—')}/5 ({comp_profile.get('app_store',{}).get('ios_reviews','—')} ratings)" if comp_profile.get("app_store",{}).get("ios_found") else "Not found"),
        stat_row("App Sentiment",      my_profile.get("app_store",{}).get("overall_sentiment","—"), comp_profile.get("app_store",{}).get("overall_sentiment","—")),
        stat_row("Reddit Mentions",    f"{my_profile.get('reddit',{}).get('total_mentions',0)} ({my_profile.get('reddit',{}).get('sentiment_score',0):+}% sentiment)", f"{comp_profile.get('reddit',{}).get('total_mentions',0)} ({comp_profile.get('reddit',{}).get('sentiment_score',0):+}% sentiment)"),
        stat_row("Cities Covered",     f"{my_profile.get('geo',{}).get('total_locations',0)} — {my_profile.get('geo',{}).get('geographic_spread','?')}", f"{comp_profile.get('geo',{}).get('total_locations',0)} — {comp_profile.get('geo',{}).get('geographic_spread','?')}"),
        stat_row("Subscription Plan",  "Yes" if my_profile.get("pricing_deep",{}).get("has_subscription") else "No", "Yes" if comp_profile.get("pricing_deep",{}).get("has_subscription") else "No"),
        stat_row("Product Areas",      str(len(my_profile.get("product",{}).get("feature_categories",[]))), str(len(comp_profile.get("product",{}).get("feature_categories",[])))),
    ]
    elements.append(Paragraph("EXECUTIVE INTELLIGENCE DASHBOARD", h2_s))
    elements.append(make_stats_table(exec_stats))
    elements.append(Spacer(1, 0.5*cm))

    # ── COMPETITIVE SCORE CHART ───────────────────────────────
    score_labels = ["Messaging","Tech Stack","Social Med.","Content","Hiring","News/PR"]
    def auto_score(profile, cat):
        if cat == "Messaging":    return 7 if profile["core"].get("meta_description") else 4
        elif cat == "Tech Stack": return min(len(profile["tech"].get("all",[])) + 2, 10)
        elif cat == "Social Med.":
            count = sum([1 if profile["facebook"].get("found") else 0, 1 if profile["linkedin"].get("found") else 0, 1 if profile["youtube"].get("found") else 0, 1 if profile["twitter"].get("found") else 0])
            return count * 2 + 2
        elif cat == "Content":  return min(profile["blog"].get("total_posts_found",0) + 2, 10)
        elif cat == "Hiring":   return min(profile["jobs"].get("total_jobs",0) + 1, 10)
        elif cat == "News/PR":  return min(profile["news"].get("total_found",0) + 1, 10)
        return 5

    my_scores   = [auto_score(my_profile,   cat) for cat in score_labels]
    comp_scores = [auto_score(comp_profile, cat) for cat in score_labels]
    elements.append(Paragraph("COMPETITIVE SCORES (Auto-calculated from data)", h2_s))
    elements.append(make_score_bar_chart(my_scores, comp_scores, score_labels, width=490, height=200))
    elements.append(Spacer(1, 0.4*cm))

    # ── TECH STACK VISUALS ────────────────────────────────────
    elements.append(Paragraph("YOUR TECH STACK", h2_s))
    elements.append(make_tech_stack_visual(my_profile["tech"], width=490, height=80))
    elements.append(Spacer(1, 0.2*cm))
    elements.append(Paragraph("COMPETITOR TECH STACK", h2_s))
    elements.append(make_tech_stack_visual(comp_profile["tech"], width=490, height=80))
    elements.append(Spacer(1, 0.3*cm))

    # ── GTM HIDDEN TOOLS ─────────────────────────────────────
    mgtm = my_profile.get("gtm",{}); cgtm = comp_profile.get("gtm",{})
    def gtm_cell(gtm_data):
        lines = []
        if gtm_data.get("gtm_ids"): lines.append(Paragraph(f"GTM ID: {', '.join(gtm_data['gtm_ids'])}", bold_s))
        hidden = gtm_data.get("hidden_tools",[])
        if hidden:
            lines.append(Paragraph(f"{len(hidden)} hidden tools:", bold_s))
            for t in hidden[:12]: lines.append(Paragraph(f"  + {t}", small_s))
        else:
            lines.append(Paragraph("No hidden tools detected", body_s))
        for e in gtm_data.get("extra_tools",[])[:4]: lines.append(Paragraph(f"  * {e}", small_s))
        return lines

    gtm_data = [
        [Paragraph("YOUR HIDDEN TECH STACK", S("GH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR HIDDEN TECH STACK", S("GH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        [gtm_cell(mgtm), gtm_cell(cgtm)],
    ]
    gtt = Table(gtm_data, colWidths=[8.7*cm,8.7*cm])
    gtt.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),LIGHTGREEN),("BACKGROUND",(1,0),(1,0),LIGHTRED),("BACKGROUND",(0,1),(0,1),HexColor("#f5fffa")),("BACKGROUND",(1,1),(1,1),HexColor("#fff8f8")),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,0),"CENTER"),("VALIGN",(0,0),(-1,-1),"TOP")]))
    elements.append(Paragraph("HIDDEN TECH STACK (Inside GTM)", h2_s))
    elements.append(gtt)
    elements.append(Spacer(1, 0.4*cm))

    # ── BLOG + NEWS SIDE BY SIDE ──────────────────────────────
    def blog_cell(profile):
        b = profile.get("blog",{}); lines = []
        if not b.get("has_blog"): lines.append(Paragraph("No blog detected", body_s))
        else:
            lines.append(Paragraph(f"Frequency: {b.get('posting_frequency')} | Posts: {b.get('total_posts_found',0)}", bold_s))
            lines.append(Paragraph(f"Found at: {', '.join(b.get('found_at_paths',[]))}", S("BL",fontSize=7,fontName="Helvetica",textColor=MUTED,spaceAfter=4)))
            for p in b.get("recent_posts",[])[:5]: lines.append(Paragraph(f"• {p['title']}", small_s))
            if b.get("content_topics"): lines.append(Paragraph(f"Topics: {', '.join(b['content_topics'][:5])}", S("BT",fontSize=7,fontName="Helvetica",textColor=BLUE,spaceAfter=0)))
        return lines

    elements.append(Paragraph("BLOG & CONTENT COMPARISON", h2_s))
    elements.append(two_col(
        [Paragraph(f"YOUR BLOG — {my_name}", S("BH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,spaceAfter=6))] + blog_cell(my_profile),
        [Paragraph(f"COMPETITOR BLOG — {comp_name}", S("BH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,spaceAfter=6))]  + blog_cell(comp_profile)
    ))
    elements.append(Spacer(1, 0.4*cm))

    def news_cell(profile):
        n = profile.get("news",{}); lines = []
        lines.append(Paragraph(f"Presence: {n.get('media_presence')} | {n.get('total_found',0)} articles", bold_s))
        for a in n.get("recent_articles",[])[:4]: lines.append(Paragraph(f"• {a['title'][:100]} [{a.get('source','')}]", small_s))
        if not n.get("recent_articles"): lines.append(Paragraph("No news found", body_s))
        signals = n.get("signals_summary",[])
        if signals: lines.append(Paragraph("Signals: " + " | ".join(signals), S("NS",fontSize=8,fontName="Helvetica-Bold",textColor=ORANGE,spaceAfter=0)))
        return lines

    elements.append(Paragraph("NEWS & PR COMPARISON", h2_s))
    elements.append(two_col(
        [Paragraph(f"YOUR NEWS — {my_name}", S("NH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,spaceAfter=6))] + news_cell(my_profile),
        [Paragraph(f"COMPETITOR NEWS — {comp_name}", S("NH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,spaceAfter=6))]  + news_cell(comp_profile)
    ))
    elements.append(Spacer(1, 0.4*cm))

    # ── SEO INTELLIGENCE ─────────────────────────────────────
    mseo = my_profile.get("seo",{}); cseo = comp_profile.get("seo",{})
    mo   = mseo.get("onpage",{}); co = cseo.get("onpage",{})
    mst  = mseo.get("sitemap",{}); cst = cseo.get("sitemap",{})
    mkw  = mseo.get("keywords",{}); ckw = cseo.get("keywords",{})
    mrob = mseo.get("robots",{}); crob = cseo.get("robots",{})

    def seo_gauge(score, grade, label, color):
        filled = int(score/10); empty = 10-filled
        bar    = "[" + ("|"*filled) + ("-"*empty) + "]"
        grade_label = "Excellent" if score>=80 else "Good" if score>=65 else "Average" if score>=50 else "Poor" if score>=35 else "Critical SEO"
        return [
            Paragraph(label, S("GL",fontSize=9,fontName="Helvetica-Bold",textColor=DARKTEXT,spaceAfter=4)),
            Paragraph(f"Score: {score}/100    Grade: {grade}", S("GS",fontSize=13,fontName="Helvetica-Bold",textColor=color,spaceAfter=6)),
            Paragraph(bar, S("GB",fontSize=14,fontName="Helvetica-Bold",textColor=color,spaceAfter=4)),
            Paragraph(grade_label, S("GL2",fontSize=8,fontName="Helvetica",textColor=MUTED,spaceAfter=0)),
        ]

    seo_gauge_data = [[seo_gauge(mo.get("score",0),mo.get("grade","?"),"YOUR SEO SCORE",GREEN), seo_gauge(co.get("score",0),co.get("grade","?"),"COMPETITOR SEO SCORE",RED)]]
    sgt = Table(seo_gauge_data, colWidths=[8.7*cm,8.7*cm])
    sgt.setStyle(TableStyle([("BACKGROUND",(0,0),(0,-1),LIGHTGREEN),("BACKGROUND",(1,0),(1,-1),LIGHTRED),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),12),("BOTTOMPADDING",(0,0),(-1,-1),12),("LEFTPADDING",(0,0),(-1,-1),12),("VALIGN",(0,0),(-1,-1),"TOP")]))
    elements.append(Paragraph("SEO INTELLIGENCE", h2_s))
    elements.append(sgt)
    elements.append(Spacer(1, 0.3*cm))

    def seo_checks_cell(onpage, robots, sitemap, keywords):
        lines = []
        lines.append(Paragraph("ON-PAGE CHECKS:", bold_s))
        for check, result in list(onpage.get("checks",{}).items())[:8]:
            clean = re.sub(r"[^\x00-\x7F]+", "", result).strip() or result[:20]
            lines.append(Paragraph(f"  {clean}  {check}", small_s))
        lines.append(Spacer(1,0.1*cm))
        lines.append(Paragraph(f"Pages in Sitemap: {sitemap.get('total_urls',0)}", bold_s))
        lines.append(Paragraph(f"SEO Footprint: {sitemap.get('seo_footprint','?')}", small_s))
        if robots.get("intelligence"):
            lines.append(Paragraph("ROBOTS.TXT SIGNALS:", bold_s))
            for intel in robots.get("intelligence",[])[:3]: lines.append(Paragraph(f"  → {intel}", small_s))
        lines.append(Spacer(1,0.1*cm))
        lines.append(Paragraph(f"Top Keywords: {', '.join(keywords.get('top_keywords_from_content',[])[:6])}", small_s))
        for signal in keywords.get("positioning_signals",[])[:2]: lines.append(Paragraph(f"  → {signal}", small_s))
        return lines

    seo_checks_data = [
        [Paragraph("YOUR SEO ANALYSIS", S("SH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR SEO ANALYSIS", S("SH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        [seo_checks_cell(mo,mrob,mst,mkw), seo_checks_cell(co,crob,cst,ckw)],
    ]
    sct = Table(seo_checks_data, colWidths=[8.7*cm,8.7*cm])
    sct.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),LIGHTGREEN),("BACKGROUND",(1,0),(1,0),LIGHTRED),("BACKGROUND",(0,1),(0,1),HexColor("#f5fffa")),("BACKGROUND",(1,1),(1,1),HexColor("#fff8f8")),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,0),"CENTER"),("VALIGN",(0,0),(-1,-1),"TOP")]))
    elements.append(sct)
    elements.append(Spacer(1, 0.3*cm))

    # Quick wins
    my_wins   = mo.get("quick_wins",[])
    comp_strs = co.get("strengths",[])
    if my_wins or comp_strs:
        qw_data = [
            [Paragraph("YOUR SEO QUICK WINS", S("QH1",fontSize=9,fontName="Helvetica-Bold",textColor=ORANGE,alignment=TA_CENTER)), Paragraph("COMPETITOR SEO STRENGTHS", S("QH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
            [[Paragraph(f"• {w}", small_s) for w in my_wins[:6]] or [Paragraph("No quick wins — great SEO!", body_s)], [Paragraph(f"• {s}", small_s) for s in comp_strs[:6]] or [Paragraph("No strengths detected", body_s)]],
        ]
        qwt = Table(qw_data, colWidths=[8.7*cm,8.7*cm])
        qwt.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),LIGHTORANGE),("BACKGROUND",(1,0),(1,0),LIGHTRED),("BACKGROUND",(0,1),(0,1),HexColor("#fffdf5")),("BACKGROUND",(1,1),(1,1),HexColor("#fff8f8")),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,0),"CENTER"),("VALIGN",(0,0),(-1,-1),"TOP")]))
        elements.append(qwt)
        elements.append(Spacer(1, 0.4*cm))

    # ── CONTACT INTELLIGENCE ─────────────────────────────────
    contact_stats = [
        [Paragraph("CONTACT",label_s), Paragraph("YOUR BUSINESS",S("YH",fontSize=8,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR",S("CH",fontSize=8,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        stat_row("Sales / Main Email",  (mct.get("sales_email","Not found") or "Not found")[:40], (cct.get("sales_email","Not found") or "Not found")[:40]),
        stat_row("Support Email",       mct.get("support_email","Not found") or "Not found",       cct.get("support_email","Not found") or "Not found"),
        stat_row("Press Email",         mct.get("press_email","Not found") or "Not found",         cct.get("press_email","Not found") or "Not found"),
        stat_row("Phone",               mct.get("phones",["Not found"])[0] if mct.get("phones") else "Not found", cct.get("phones",["Not found"])[0] if cct.get("phones") else "Not found"),
        stat_row("All Emails Found",    (", ".join(mct.get("emails",[])[:2]) or "None")[:45],      (", ".join(cct.get("emails",[])[:2]) or "None")[:45]),
        stat_row("CEO Email",           mct.get("ceo_email","Not found") or "Not found",           cct.get("ceo_email","Not found") or "Not found"),
        stat_row("Named Contacts",      str(len(mct.get("named_contacts",[])))+" found",           str(len(cct.get("named_contacts",[])))+" found"),
        stat_row("Data Source",         "Hunter.io (verified)" if mct.get("hunter_verified") else "Scraped", "Hunter.io (verified)" if cct.get("hunter_verified") else "Scraped"),
    ]
    elements.append(Paragraph("CONTACT INTELLIGENCE", h2_s))
    elements.append(make_stats_table(contact_stats))
    elements.append(Spacer(1, 0.5*cm))

    # ── APP STORE INTELLIGENCE ───────────────────────────────
    def app_cell(app):
        lines = []
        if app.get("ios_found"):
            lines.append(Paragraph(f"iOS: {app.get('ios_rating','?')}/5 — {app.get('ios_reviews',0):,} ratings", bold_s))
            lines.append(Paragraph(f"  Version: {app.get('ios_version','?')} | Updated: {app.get('ios_updated','?')} | Size: {app.get('ios_size','?')}", small_s))
        else:
            lines.append(Paragraph("iOS: Not found", body_s))
        if app.get("android_found"):
            lines.append(Paragraph(f"Android: {app.get('android_rating','?')}/5 | Installs: {app.get('android_installs','?')}", bold_s))
        else:
            lines.append(Paragraph("Android: Not found", body_s))
        if app.get("overall_sentiment") != "Unknown": lines.append(Paragraph(f"Review Sentiment: {app.get('overall_sentiment','?')}", bold_s))
        if app.get("top_complaints"): lines.append(Paragraph(f"Top complaints: {', '.join(app['top_complaints'][:6])}", small_s))
        if app.get("top_praises"):    lines.append(Paragraph(f"Top praises: {', '.join(app['top_praises'][:6])}", small_s))
        if app.get("feature_mentions"): lines.append(Paragraph(f"Features: {', '.join(app['feature_mentions'][:6])}", small_s))
        if not any([app.get("ios_found"),app.get("android_found")]): lines.append(Paragraph("No app store data found", body_s))
        return lines

    app_data = [
        [Paragraph("YOUR APP STORE",S("APH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR APP STORE",S("APH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        [app_cell(my_profile.get("app_store",{})), app_cell(comp_profile.get("app_store",{}))],
    ]
    apt = Table(app_data, colWidths=[8.7*cm,8.7*cm], splitByRow=True)
    apt.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),LIGHTGREEN),("BACKGROUND",(1,0),(1,0),LIGHTRED),("BACKGROUND",(0,1),(0,1),HexColor("#f5fffa")),("BACKGROUND",(1,1),(1,1),HexColor("#fff8f8")),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,0),"CENTER"),("VALIGN",(0,0),(-1,-1),"TOP")]))
    elements.append(Paragraph("APP STORE INTELLIGENCE", h2_s))
    elements.append(apt)
    elements.append(Spacer(1, 0.4*cm))

    # ── REDDIT + PRICING + GEO (condensed) ───────────────────
    def reddit_cell(red):
        lines = []
        if red.get("total_mentions",0) == 0:
            lines.append(Paragraph("No Reddit mentions found", body_s)); return lines
        lines.append(Paragraph(f"Total mentions: {red.get('total_mentions',0)} | Sentiment: {red.get('sentiment_score',0):+}%", bold_s))
        lines.append(Paragraph(f"Positive: {red.get('positive_mentions',0)} | Negative: {red.get('negative_mentions',0)}", small_s))
        if red.get("top_subreddits"):   lines.append(Paragraph(f"Top subreddits: {', '.join(red['top_subreddits'][:4])}", small_s))
        if red.get("trending_topics"):  lines.append(Paragraph(f"Trending: {', '.join(red['trending_topics'][:6])}", small_s))
        for post in red.get("recent_posts",[])[:4]:
            icon = "[+]" if post["sentiment"]=="positive" else "[-]" if post["sentiment"]=="negative" else "[~]"
            lines.append(Paragraph(f"  {icon} {post['title'][:80]}", small_s))
        return lines

    red_data = [
        [Paragraph("YOUR REDDIT MENTIONS",S("RDH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR REDDIT MENTIONS",S("RDH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        [reddit_cell(my_profile.get("reddit",{})), reddit_cell(comp_profile.get("reddit",{}))],
    ]
    rdt = Table(red_data, colWidths=[8.7*cm,8.7*cm], splitByRow=True)
    rdt.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),LIGHTGREEN),("BACKGROUND",(1,0),(1,0),LIGHTRED),("BACKGROUND",(0,1),(0,1),HexColor("#f5fffa")),("BACKGROUND",(1,1),(1,1),HexColor("#fff8f8")),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,0),"CENTER"),("VALIGN",(0,0),(-1,-1),"TOP")]))
    elements.append(Paragraph("REDDIT & SOCIAL SENTIMENT", h2_s))
    elements.append(rdt)
    elements.append(Spacer(1, 0.4*cm))

    def pricing_cell(pr):
        lines = []
        lines.append(Paragraph(f"Model: {pr.get('pricing_model','Unknown')}", bold_s))
        lines.append(Paragraph(f"Strategy: {pr.get('pricing_strategy','Unknown')}", small_s))
        if pr.get("has_subscription"):
            lines.append(Paragraph(f"Subscription: Yes", bold_s))
            for plan in pr.get("subscription_plans",[])[:3]: lines.append(Paragraph(f"  Plan: {plan[:60]}", small_s))
        if pr.get("price_points"):    lines.append(Paragraph(f"Price points: {', '.join(pr['price_points'][:5])}", small_s))
        if pr.get("delivery_fees"):   lines.append(Paragraph(f"Delivery fees: {pr['delivery_fees'][0][:70]}", small_s))
        if pr.get("commission_rate"): lines.append(Paragraph(f"Commission rate: {pr['commission_rate']}", bold_s))
        if pr.get("free_tier"):       lines.append(Paragraph(f"Free tier: Yes — {(pr.get('free_tier_details') or '')[:60]}", small_s))
        if pr.get("current_offers"):  lines.append(Paragraph(f"Current offers: {', '.join(pr['current_offers'][:4])}", small_s))
        for b in pr.get("membership_benefits",[])[:3]: lines.append(Paragraph(f"  + {b[:70]}", small_s))
        if not any([pr.get("has_subscription"),pr.get("price_points"),pr.get("delivery_fees")]): lines.append(Paragraph("No pricing data found", body_s))
        return lines

    pr_data = [
        [Paragraph("YOUR PRICING",S("PRH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR PRICING",S("PRH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        [pricing_cell(my_profile.get("pricing_deep",{})), pricing_cell(comp_profile.get("pricing_deep",{}))],
    ]
    prt = Table(pr_data, colWidths=[8.7*cm,8.7*cm], splitByRow=True)
    prt.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),LIGHTGREEN),("BACKGROUND",(1,0),(1,0),LIGHTRED),("BACKGROUND",(0,1),(0,1),HexColor("#f5fffa")),("BACKGROUND",(1,1),(1,1),HexColor("#fff8f8")),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,0),"CENTER"),("VALIGN",(0,0),(-1,-1),"TOP")]))
    elements.append(Paragraph("PRICING INTELLIGENCE", h2_s))
    elements.append(prt)
    elements.append(Spacer(1, 0.4*cm))

    # Geo + Product
    mgeo = my_profile.get("geo",{}); cgeo = comp_profile.get("geo",{})
    mprd = my_profile.get("product",{}); cprd = comp_profile.get("product",{})
    geo_data = [
        [Paragraph("YOUR GEOGRAPHIC FOOTPRINT",S("GH1",fontSize=9,fontName="Helvetica-Bold",textColor=GREEN,alignment=TA_CENTER)), Paragraph("COMPETITOR GEOGRAPHIC FOOTPRINT",S("GH2",fontSize=9,fontName="Helvetica-Bold",textColor=RED,alignment=TA_CENTER))],
        [
            [Paragraph(f"Spread: {mgeo.get('geographic_spread','Unknown')}", bold_s), Paragraph(f"Cities: {', '.join(mgeo.get('cities_detected',[])[:8]) or 'None detected'}", small_s), Paragraph(f"Markets: {', '.join(mgeo.get('countries_detected',[])[:5]) or 'Unknown'}", small_s), Spacer(1,0.1*cm), Paragraph("PRODUCT AREAS:", bold_s)] + [Paragraph(f"  + {f}", small_s) for f in mprd.get("feature_categories",[])[:6]],
            [Paragraph(f"Spread: {cgeo.get('geographic_spread','Unknown')}", bold_s), Paragraph(f"Cities: {', '.join(cgeo.get('cities_detected',[])[:8]) or 'None detected'}", small_s), Paragraph(f"Markets: {', '.join(cgeo.get('countries_detected',[])[:5]) or 'Unknown'}", small_s), Spacer(1,0.1*cm), Paragraph("PRODUCT AREAS:", bold_s)] + [Paragraph(f"  + {f}", small_s) for f in cprd.get("feature_categories",[])[:6]],
        ],
    ]
    get_t = Table(geo_data, colWidths=[8.7*cm,8.7*cm], splitByRow=True)
    get_t.setStyle(TableStyle([("BACKGROUND",(0,0),(0,0),LIGHTGREEN),("BACKGROUND",(1,0),(1,0),LIGHTRED),("BACKGROUND",(0,1),(0,1),HexColor("#f5fffa")),("BACKGROUND",(1,1),(1,1),HexColor("#fff8f8")),("BOX",(0,0),(-1,-1),1,BORDER),("INNERGRID",(0,0),(-1,-1),0.5,BORDER),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),8),("ALIGN",(0,0),(-1,0),"CENTER"),("VALIGN",(0,0),(-1,-1),"TOP")]))
    elements.append(Paragraph("GEOGRAPHIC FOOTPRINT + PRODUCT INTELLIGENCE", h2_s))
    elements.append(get_t)
    elements.append(Spacer(1, 0.5*cm))

    # ── AI REPORT SECTIONS ───────────────────────────────────
    def flush(section, lines):
        if not section or not lines: return
        icon, bg, acc = SECTIONS.get(section, ("[S]", LIGHT, DARKTEXT))
        elements.append(sec_hdr(icon, section, acc))
        content_paras = []
        for line in lines:
            line = line.strip()
            if not line:
                content_paras.append(Spacer(1, 0.06*cm)); continue
            if re.match(r"^\d+[\.\)]", line):         content_paras.append(Paragraph(line, num_s))
            elif re.match(r"^(Week|Action|Day)\s*\d", line, re.I): content_paras.append(Paragraph(line, bold_s))
            elif re.match(r"^[A-Z][a-zA-Z\s]+:\s", line) and len(line) < 80: content_paras.append(Paragraph(line, bold_s))
            else:                                      content_paras.append(Paragraph(line, body_s))
        if not content_paras: return
        for i in range(0, len(content_paras), 25):
            chunk = content_paras[i:i+25]
            t = Table([[chunk]], colWidths=[17.4*cm])
            t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),bg),("TOPPADDING",(0,0),(-1,-1),8),("BOTTOMPADDING",(0,0),(-1,-1),8),("LEFTPADDING",(0,0),(-1,-1),14),("RIGHTPADDING",(0,0),(-1,-1),14),("LINEBELOW",(0,0),(-1,-1),0.5,BORDER)]))
            elements.append(t)
        elements.append(Spacer(1, 0.35*cm))

    current = None; lines = []
    for line in report_text.split("\n"):
        up = line.strip().upper(); matched = None
        for key in SECTIONS:
            if key in up and len(up) < len(key)+8: matched = key; break
        if matched:
            flush(current, lines[:150])
            current = matched; lines = []
        else:
            lines.append(line)
    flush(current, lines[:150])

    # ── ABOUT PAGE ────────────────────────────────────────────
    elements.append(PageBreak())
    elements.append(Spacer(1, 2*cm))
    elements.append(dark_box(Paragraph("ABOUT RIVALSCAN.AI", S("ABT",fontSize=20,fontName="Helvetica-Bold",textColor=WHITE,alignment=TA_CENTER)),pt=20,pb=20))
    elements.append(Spacer(1, 0.5*cm))
    about_sections = [
        ("What Is RivalScan.ai?", "RivalScan.ai is an AI-powered competitive intelligence platform that automatically collects, analyzes, and synthesizes publicly available data about any company into a comprehensive business intelligence report. Reports are generated in minutes, not weeks."),
        ("What Data Sources Do We Use?", "All data is sourced from publicly available information including: company websites, Google Tag Manager signals, sitemap files, robots.txt, LinkedIn, Facebook, YouTube, Twitter/X, Google News, Bing News, job listing platforms, and public review sites. No private or proprietary data is accessed."),
        ("How Accurate Is This Report?", "Data accuracy depends on what companies publish publicly. Tech stack detection via GTM deep scanning is highly reliable. Social media metrics are scraped in real-time. News data is current as of report generation. SEO scores follow industry-standard on-page metrics. AI analysis is generated by Claude (Anthropic). Note: JS-rendered websites return limited homepage data."),
        ("How Often Should I Run This Report?", "We recommend monthly reports for ongoing competitive monitoring. The competitive landscape changes continuously — pricing, hiring, tech stack, and content strategy all shift. Monthly monitoring ensures you never miss a strategic move."),
        ("Disclaimer", "This report is generated using publicly available data and AI analysis. RivalScan.ai is not responsible for business decisions made based on this report. Users should independently verify key findings. All company names and trademarks remain the property of their respective owners."),
    ]
    for title, body in about_sections:
        elements.append(Paragraph(title, S("ABH",fontSize=11,fontName="Helvetica-Bold",textColor=DARKTEXT,spaceBefore=16,spaceAfter=4)))
        elements.append(Paragraph(body,  S("ABB",fontSize=9,fontName="Helvetica",textColor=HexColor("#444444"),leading=14,spaceAfter=4)))
    elements.append(Spacer(1, 1*cm))
    elements.append(dark_box(Paragraph("rivalscan.ai  |  intelligence@rivalscan.ai  |  Confidential", S("ABFT",fontSize=9,fontName="Helvetica",textColor=HexColor("#aaaaaa"),alignment=TA_CENTER)),pt=12,pb=12))

    doc.build(elements, canvasmaker=NumberedCanvas)
    print(f"  ✅ PDF built: {filename}")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════

def run(my_url, comp_url):
    print(f"\n{'🚀'*20}")
    print(f"  RIVALSCAN MASTER v12 — Complete Intelligence")
    print(f"  {datetime.now().strftime('%B %d, %Y — %H:%M')}")
    print(f"{'🚀'*20}\n")

    print(f"\n{'═'*55}")
    print(f"  PHASE 1 — YOUR COMPANY PROFILE")
    print(f"{'═'*55}")
    my_profile = build_profile(my_url)

    print(f"\n{'═'*55}")
    print(f"  PHASE 2 — COMPETITOR PROFILE")
    print(f"{'═'*55}")
    comp_profile = build_profile(comp_url)

    print(f"\n{'═'*55}")
    print(f"  PHASE 3 — AI MASTER ANALYSIS")
    print(f"{'═'*55}")
    report = generate_master_report(my_profile, comp_profile)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_file  = f"rivalscan_MASTER_{timestamp}.pdf"
    txt_file  = f"rivalscan_MASTER_{timestamp}.txt"

    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(f"RIVALSCAN MASTER v12\n{my_url} vs {comp_url}\n")
        f.write(f"Generated: {datetime.now().strftime('%B %d, %Y')}\n{'='*55}\n\n")
        f.write(report)

    print(f"\n{'═'*55}")
    print(f"  PHASE 4 — BUILDING MASTER PDF")
    print(f"{'═'*55}")
    build_master_pdf(my_url, comp_url, my_profile, comp_profile, report, pdf_file)

    print(f"\n{'='*55}")
    print(f"✅ MASTER PDF:  {pdf_file}")
    print(f"✅ TEXT BACKUP: {txt_file}")
    print(f"{'='*55}")
    print(f"\n🎯 Charge $199 per report.")


if __name__ == "__main__":
    print("\n🔍 RIVALSCAN MASTER v12 — Business Intelligence")
    print("="*55)
    my_url   = input("\nEnter YOUR website URL: ").strip()
    comp_url = input("Enter COMPETITOR URL:   ").strip()
    if not my_url.startswith("http"):   my_url   = "https://" + my_url
    if not comp_url.startswith("http"): comp_url = "https://" + comp_url
    run(my_url, comp_url)