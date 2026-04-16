"""RivalScan v12 - Part 1: Utilities + Core Scraping Modules 1-6"""

import re, json, time, requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, quote
from dotenv import load_dotenv
from collections import Counter
import os

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
HUNTER_API_KEY    = os.getenv("HUNTER_API_KEY", "")
GOOGLE_API_KEY    = os.getenv("GOOGLE_API_KEY", "")
NEWS_API_KEY      = os.getenv("NEWS_API_KEY", "")

PAGESPEED_URL   = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
HEADERS_MOBILE = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ═══════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════

def safe_get(url, timeout=12, mobile=False):
    try:
        h = HEADERS_MOBILE if mobile else HEADERS
        r = requests.get(url, headers=h, timeout=timeout)
        r.raise_for_status()
        return r
    except:
        return None

def extract_domain(url):
    return urlparse(url).netloc.replace("www.", "")

def extract_company_name(url, soup=None):
    domain = extract_domain(url)
    name   = domain.split(".")[0].capitalize()
    if soup and soup.title:
        title = (soup.title.string or "").strip()
        parts = re.split(r"[|\-–—]", title)
        if parts:
            candidate = re.sub(r"[^a-zA-Z0-9\s\.\-&]", "", parts[0]).strip()
            if candidate and len(candidate) < 40:
                name = candidate
    return name.strip()

def clean_name_for_display(name, url):
    domain = extract_domain(url)
    if name.lower() == domain.split(".")[0].lower():
        name = name + "." + domain.split(".")[-1]
    return name

def detect_country(domain):
    tld = domain.split(".")[-1]
    return {
        "ae":"UAE","uk":"UK","au":"Australia","in":"India","sg":"Singapore",
        "sa":"Saudi Arabia","qa":"Qatar","bh":"Bahrain","kw":"Kuwait",
        "eg":"Egypt","pk":"Pakistan","ng":"Nigeria",
    }.get(tld, "")


# ═══════════════════════════════════════════════════
# MODULE 1: WEBSITE CORE
# ═══════════════════════════════════════════════════

def scrape_website(url):
    print(f"  🌐 Website: {url}")
    r = safe_get(url)
    domain = extract_domain(url)
    if not r:
        return {
            "url": url, "error": "Could not fetch",
            "company_name": extract_company_name(url),
            "domain": domain, "country": detect_country(domain),
            "title": "", "meta_description": "", "headings": [],
            "value_proposition": "", "cta_buttons": [], "pricing_mentions": [],
            "social_links": {}, "navigation_items": [], "main_content": "",
            "js_rendered": False,
        }

    soup = BeautifulSoup(r.text, "html.parser")

    is_js_rendered = (
        len(r.text) < 5000 or
        "You need to enable JavaScript" in r.text or
        ("__NEXT_DATA__" in r.text and len(soup.get_text(strip=True)) < 200) or
        ("ng-version" in r.text and len(soup.get_text(strip=True)) < 200)
    )

    for tag in soup(["script", "style", "iframe"]):
        tag.decompose()

    raw_title = soup.title.string.strip() if soup.title and soup.title.string else ""

    data = {
        "url": url, "domain": domain,
        "company_name": extract_company_name(url, soup),
        "title": raw_title, "js_rendered": is_js_rendered,
        "meta_description": "", "headings": [], "value_proposition": "",
        "cta_buttons": [], "pricing_mentions": [], "social_links": {},
        "navigation_items": [], "main_content": "", "country": detect_country(domain),
    }

    meta = soup.find("meta", attrs={"name": "description"})
    if meta:
        data["meta_description"] = meta.get("content", "")

    for tag in ["h1", "h2", "h3"]:
        for h in soup.find_all(tag)[:8]:
            t = h.get_text(strip=True)
            if t and len(t) > 3:
                data["headings"].append(t)

    if data["headings"]:
        data["value_proposition"] = data["headings"][0]

    nav = soup.find("nav")
    if nav:
        for a in nav.find_all("a")[:12]:
            t = a.get_text(strip=True)
            if t:
                data["navigation_items"].append(t)

    for p in soup.find_all(["p", "span", "div", "li"]):
        t = p.get_text(strip=True)
        if any(k in t.lower() for k in ["$", "price", "plan", "free", "month", "year", "pricing", "aed", "usd"]) and 5 < len(t) < 200:
            data["pricing_mentions"].append(t)
            if len(data["pricing_mentions"]) >= 5:
                break

    social_map = {
        "twitter": ["twitter.com", "x.com"], "linkedin": ["linkedin.com"],
        "instagram": ["instagram.com"], "facebook": ["facebook.com"],
        "youtube": ["youtube.com"], "tiktok": ["tiktok.com"], "github": ["github.com"],
    }
    for a in soup.find_all("a", href=True):
        href = a["href"]
        for platform, patterns in social_map.items():
            if any(p in href for p in patterns):
                data["social_links"][platform] = href
                break

    body = soup.get_text(separator=" ", strip=True)
    data["main_content"] = " ".join(body.split())[:3000]

    status = "[JS-rendered — limited data]" if is_js_rendered else ""
    print(f"     ✅ {data['company_name']} | {domain} | {data['country'] or 'Global'} {status}")
    return data


# ═══════════════════════════════════════════════════
# MODULE 2: TECH STACK
# ═══════════════════════════════════════════════════

def detect_tech_stack(url):
    print(f"  🔧 Tech Stack...")
    r    = safe_get(url)
    html = r.text.lower() if r else ""
    tech = {"cms": [], "analytics": [], "marketing": [], "ads": [], "payments": [], "support": [], "hosting": [], "all": []}

    detections = {
        "WordPress":         (["wp-content","wp-includes","wp-json","xmlrpc.php"], "cms"),
        "Shopify":           (["cdn.shopify","shopify.com","myshopify"], "cms"),
        "Webflow":           (["webflow.com","webflow.io","assets-global.website"], "cms"),
        "Wix":               (["wixstatic","wix.com","parastorage"], "cms"),
        "Squarespace":       (["squarespace.com","sqspcdn"], "cms"),
        "HubSpot CMS":       (["hs-scripts","hubspot.com/hs","hs-analytics"], "cms"),
        "Drupal":            (["drupal.js","drupal.settings","sites/default/files"], "cms"),
        "Joomla":            (["joomla","com_content","option=com_"], "cms"),
        "Google Analytics":  (["gtag/js","google-analytics","ga.js","analytics.js","UA-","G-"], "analytics"),
        "Google Tag Manager":(["googletagmanager.com/gtm","GTM-"], "analytics"),
        "Mixpanel":          (["mixpanel.com","mixpanel.init"], "analytics"),
        "Hotjar":            (["hotjar.com","hjSiteSettings","hj("], "analytics"),
        "Heap":              (["heapanalytics","heap.load"], "analytics"),
        "Segment":           (["segment.com","segment.io","analytics.load"], "analytics"),
        "Clarity":           (["clarity.ms","clarity("], "analytics"),
        "Amplitude":         (["amplitude.com","amplitude.getInstance"], "analytics"),
        "FullStory":         (["fullstory.com","fs.identify"], "analytics"),
        "HubSpot":           (["hubspot.com","hs-banner","hsforms","leadin"], "marketing"),
        "Mailchimp":         (["mailchimp.com","chimpstatic","list-manage"], "marketing"),
        "Klaviyo":           (["klaviyo.com","kl_"], "marketing"),
        "Salesforce":        (["salesforce.com","force.com","pardot","exacttarget"], "marketing"),
        "ActiveCampaign":    (["activecampaign.com","trackcmp"], "marketing"),
        "Marketo":           (["marketo.net","munchkin"], "marketing"),
        "Google Ads":        (["googleadservices","google_tag","conversion.js","adwords"], "ads"),
        "Facebook Pixel":    (["connect.facebook.net","fbq(","facebook.com/tr"], "ads"),
        "LinkedIn Ads":      (["snap.licdn","linkedin.com/insight","_linkedin_partner"], "ads"),
        "Twitter Ads":       (["static.ads-twitter","twq("], "ads"),
        "TikTok Pixel":      (["analytics.tiktok","ttq."], "ads"),
        "Criteo":            (["criteo.com","criteo.net"], "ads"),
        "Stripe":            (["stripe.com","js.stripe","stripe.js"], "payments"),
        "PayPal":            (["paypal.com","paypalobjects"], "payments"),
        "Checkout.com":      (["checkout.com","cko-"], "payments"),
        "Adyen":             (["adyen.com"], "payments"),
        "Chargebee":         (["chargebee.com"], "payments"),
        "Intercom":          (["intercom.io","widget.intercom","intercomSettings"], "support"),
        "Zendesk":           (["zendesk.com","zdassets","zE("], "support"),
        "Freshdesk":         (["freshdesk.com","freshchat","fcWidget"], "support"),
        "Tawk.to":           (["tawk.to","tawkto"], "support"),
        "Crisp":             (["crisp.chat"], "support"),
        "LiveChat":          (["livechat.com","livechatinc"], "support"),
        "AWS":               (["amazonaws.com","cloudfront.net","s3."], "hosting"),
        "Cloudflare":        (["cloudflare.com","__cf_bm","cdn-cgi"], "hosting"),
        "Vercel":            (["vercel.com","vercel.app","_vercel"], "hosting"),
        "Netlify":           (["netlify.com","netlify.app"], "hosting"),
        "Azure":             (["azurewebsites","azure.com","azureedge"], "hosting"),
        "Google Cloud":      (["storage.googleapis","googleusercontent","appspot.com"], "hosting"),
    }

    for tool, (signals, category) in detections.items():
        if any(s in html for s in signals):
            tech[category].append(tool)
            tech["all"].append(tool)

    print(f"     ✅ {len(tech['all'])} tools: {', '.join(tech['all'][:6]) or 'None detected'}")
    return tech


# ═══════════════════════════════════════════════════
# MODULE 3: LEADERSHIP
# ═══════════════════════════════════════════════════

def scrape_leadership(base_url):
    print(f"  👤 Leadership...")
    data = {"ceo": None, "founders": [], "team_members": [], "team_size_hint": "Unknown", "about_page": None}

    for path in ["/about", "/about-us", "/team", "/our-team", "/leadership", "/founders", "/company"]:
        url  = base_url.rstrip("/") + path
        r    = safe_get(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()

        for pat in [
            r"([A-Z][a-z]+ [A-Z][a-z]+),?\s*(?:is\s+)?(?:the\s+)?(?:CEO|Chief Executive Officer|Co-Founder & CEO|Founder & CEO)",
            r"(?:CEO|Chief Executive Officer)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)",
            r"([A-Z][a-z]+ [A-Z][a-z]+)\s*[\|,]\s*(?:CEO|Founder|Co-Founder)",
        ]:
            m = re.search(pat, text)
            if m:
                data["ceo"] = m.group(1).strip()
                break

        founders = re.findall(r"([A-Z][a-z]+ [A-Z][a-z]+),?\s*(?:is\s+)?(?:a\s+)?(?:Co-)?Founder", text)
        data["founders"] = list(set(founders))[:4]

        for section in soup.find_all(["div","section"], class_=re.compile(r"team|member|person|staff|founder|leadership", re.I))[:5]:
            for n in section.find_all(["h3","h4","h2","strong"])[:10]:
                name = n.get_text(strip=True)
                if re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+$", name):
                    data["team_members"].append(name)

        if data["ceo"] or data["founders"] or data["team_members"]:
            data["about_page"] = url
            size = len(data["team_members"])
            data["team_size_hint"] = (
                "Large (10+)" if size >= 10 else
                "Mid (5-9)"   if size >= 5  else
                "Small (2-4)" if size >= 2  else
                "Unknown"
            )
            break

    print(f"     ✅ CEO: {data['ceo'] or 'Not public'} | Team: {data['team_size_hint']}")
    return data


# ═══════════════════════════════════════════════════
# MODULE 4: CONTACT
# ═══════════════════════════════════════════════════

def scrape_contact(base_url, existing_emails=None):
    print(f"  📞 Contact...")
    contact = {
        "emails": existing_emails or [], "phones": [],
        "sales_email": None, "support_email": None,
        "press_email": None, "main_email": None,
    }

    pages = [
        base_url,
        base_url.rstrip("/") + "/contact",
        base_url.rstrip("/") + "/contact-us",
        base_url.rstrip("/") + "/support",
        base_url.rstrip("/") + "/help",
        base_url.rstrip("/") + "/about",
        base_url.rstrip("/") + "/about-us",
        base_url.rstrip("/") + "/team",
    ]

    all_emails = list(existing_emails or [])
    all_phones = []

    for url in pages:
        r = safe_get(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()

        mailto_emails = re.findall(r"mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6})", text, re.I)
        raw_emails    = re.findall(r"\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,6})\b", text)

        for e in list(set(raw_emails + mailto_emails)):
            e = e.strip()
            if "@" not in e or len(e) > 60:
                continue
            if any(x in e.lower() for x in [".png",".jpg",".woff","@2x","sentry","example","placeholder","noreply","no-reply","donotreply"]):
                continue
            if len(e.split("@")[-1].split(".")[-1]) >= 2:
                all_emails.append(e)

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                if email and "@" in email:
                    all_emails.append(email)

        phones = re.findall(r"(?:\+?[0-9]{1,3}[\s\-]?)?(?:\(?[0-9]{3,4}\)?[\s\-]?){2,4}[0-9]{3,4}", text)
        all_phones.extend([p.strip() for p in phones if len(re.sub(r"[^0-9]","",p)) >= 7])

    tlds = {"com","ae","uk","org","net","io","co","sa","qa","bh","kw","eg","in","sg","au","us","gov","edu"}
    cleaned = []
    for e in all_emails:
        e = e.strip()
        e = re.split(r"[\s'\"<>\(\)\[\]{}|\\]", e)[0]
        if "@" not in e:
            continue
        local, dom = e.rsplit("@", 1)
        tld = dom.split(".")[-1].lower()
        if tld in tlds and len(e) < 65 and "." in dom:
            cleaned.append(e.lower())

    contact["emails"] = list(dict.fromkeys(cleaned))[:8]
    contact["phones"] = list(dict.fromkeys(all_phones))[:5]

    domain = extract_domain(base_url)
    for email in contact["emails"]:
        el = email.lower()
        if domain not in el:
            continue
        if any(k in el for k in ["support","help","care","service"]):
            contact["support_email"] = email
        elif any(k in el for k in ["press","media","pr","news"]):
            contact["press_email"] = email
        elif any(k in el for k in ["sales","growth","revenue","bd","business"]):
            contact["sales_email"] = contact["sales_email"] or email
        elif any(k in el for k in ["ask","hello","hi","info","contact","enquir"]):
            if not contact["sales_email"]:
                contact["sales_email"] = email
        if not contact["main_email"] and domain in el:
            contact["main_email"] = email

    if not contact["sales_email"]:
        contact["sales_email"] = contact["main_email"] or (next((e for e in contact["emails"] if domain in e.lower()), None))

    found = len(contact["emails"])
    phone = contact["phones"][0] if contact["phones"] else "None found"
    print(f"     ✅ {found} email(s) | Main: {contact.get('sales_email') or 'None'} | Phone: {phone}")
    return contact


# ═══════════════════════════════════════════════════
# MODULE 5: HIRING
# ═══════════════════════════════════════════════════

def scrape_jobs(base_url, company_name):
    print(f"  💼 Hiring...")
    jobs = {
        "is_hiring": False, "jobs_page": None, "open_positions": [],
        "departments": {}, "total_jobs": 0,
        "hiring_signal": "Not hiring or jobs not public", "strategic_insight": "",
    }
    DEPT_MAP = {
        "Engineering": ["engineer","developer","architect","devops","data","ml","ai","backend","frontend"],
        "Sales":       ["sales","account","revenue","bdr","sdr","business development"],
        "Marketing":   ["marketing","growth","seo","content","brand","demand gen"],
        "Product":     ["product","ux","design","research","ui"],
        "Operations":  ["operations","finance","legal","hr","support","success","ops"],
    }

    for path in ["/careers","/jobs","/work-with-us","/join-us","/hiring","/join","/vacancies","/opportunities","/openings","/recruitment","/career","/job-openings"]:
        url  = base_url.rstrip("/") + path
        r    = safe_get(url)
        if not r:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        positions = []
        job_kw = ["Engineer","Developer","Designer","Manager","Analyst","Scientist","Marketing","Sales","Product","Operations","Finance","Legal","HR","Support","Director","Head","VP","Specialist"]

        for el in soup.find_all(["h2","h3","h4","li","a"]):
            t = el.get_text(strip=True)
            if 5 < len(t) < 80 and any(k.lower() in t.lower() for k in job_kw):
                positions.append(t)

        if positions:
            jobs["is_hiring"]      = True
            jobs["jobs_page"]      = url
            jobs["open_positions"] = list(set(positions))[:20]
            jobs["total_jobs"]     = len(jobs["open_positions"])
            pos_text               = " ".join(jobs["open_positions"]).lower()

            for dept, kws in DEPT_MAP.items():
                count = sum(1 for kw in kws if kw in pos_text)
                if count > 0:
                    jobs["departments"][dept] = count

            count = jobs["total_jobs"]
            jobs["hiring_signal"] = (
                f"Aggressively hiring ({count} roles)" if count >= 10 else
                f"Actively growing ({count} roles)"    if count >= 5  else
                f"Steady hiring ({count} roles)"       if count >= 2  else
                f"Minimal hiring ({count} roles)"
            )
            depts = list(jobs["departments"].keys())
            jobs["strategic_insight"] = (
                f"Hiring in: {', '.join(depts)}. "
                + ("Product expansion incoming. " if "Engineering" in depts else "")
                + ("Growth/sales push. "          if "Sales"       in depts else "")
                + ("Brand or launch campaign. "   if "Marketing"   in depts else "")
            )
            break

    print(f"     ✅ {jobs['hiring_signal']}")
    return jobs


# ═══════════════════════════════════════════════════
# MODULE 6: BLOG
# ═══════════════════════════════════════════════════

def scrape_blog(base_url):
    print(f"  📝 Blog...")
    PATHS = [
        "/blog","/news","/insights","/resources","/articles","/updates","/posts",
        "/content","/media","/press","/newsroom","/learn","/knowledge",
        "/whitepapers","/case-studies","/stories","/journal","/hub",
    ]
    domain = extract_domain(base_url)
    subdomain_urls = [f"https://blog.{domain}", f"https://news.{domain}", f"https://insights.{domain}"]

    all_posts = []
    found_paths = []

    for sub_url in subdomain_urls:
        r = safe_get(sub_url)
        if not r or len(r.text) < 500:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        arts = soup.find_all(["article","div","li"], class_=re.compile(r"post|article|blog|entry|card|news", re.I))[:15]
        if len(arts) >= 2:
            found_paths.append(sub_url)
            for art in arts[:8]:
                te = art.find(["h1","h2","h3","h4","a"])
                t  = te.get_text(strip=True) if te else ""
                if t and len(t) > 8:
                    all_posts.append({"title": t[:120], "date": "", "source": sub_url})

    for path in PATHS:
        url  = base_url.rstrip("/") + path
        r    = safe_get(url)
        if not r or len(r.text) < 500:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        arts = soup.find_all(["article","div","li"], class_=re.compile(r"post|article|blog|entry|card|news", re.I))[:15]

        if len(arts) < 2:
            links = [a for a in soup.find_all("a", href=True) if any(k in a.get("href","").lower() for k in ["/blog/","/news/","/article/","/post/"])]
            if len(links) >= 2:
                for a in links[:8]:
                    t = a.get_text(strip=True)
                    if t and len(t) > 10:
                        all_posts.append({"title": t[:120], "date": "", "source": path})
                if all_posts:
                    found_paths.append(path)
            continue

        found_paths.append(path)
        for art in arts[:8]:
            te = art.find(["h1","h2","h3","h4","a"])
            de = art.find(["time","span"], class_=re.compile(r"date|time|published", re.I))
            t  = te.get_text(strip=True) if te else ""
            d  = de.get_text(strip=True) if de else ""
            if t and len(t) > 8:
                all_posts.append({"title": t[:120], "date": d[:30], "source": path})

    seen = set()
    unique = []
    for p in all_posts:
        if p["title"] not in seen:
            seen.add(p["title"])
            unique.append(p)

    count  = len(unique)
    freq   = "Very Active" if count>=10 else "Active" if count>=5 else "Moderate" if count>=2 else "Inactive"
    topics = list(set(re.findall(r"\b[A-Z][a-z]{3,}(?:\s[A-Z][a-z]{3,})*\b", " ".join([p["title"] for p in unique]))))[:10]

    print(f"     ✅ {count} posts | {freq} | Paths: {', '.join(found_paths) or 'None'}")
    return {
        "has_blog": count > 0, "found_at_paths": found_paths,
        "recent_posts": unique[:12], "total_posts_found": count,
        "posting_frequency": freq, "content_topics": topics,
    }
