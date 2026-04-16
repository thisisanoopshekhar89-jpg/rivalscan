"""RivalScan v12 - Part 3: API Modules + New Intelligence Modules"""

import re, requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from collections import Counter
from part1_utils_scraping import (
    safe_get, extract_domain, detect_country,
    HUNTER_API_KEY, GOOGLE_API_KEY, NEWS_API_KEY,
    PAGESPEED_URL, YOUTUBE_API_URL, HEADERS
)


# ═══════════════════════════════════════════════════
# DOMAIN INTELLIGENCE
# ═══════════════════════════════════════════════════

def scan_domain_intelligence(url, domain):
    print(f"  🌍 Domain Intelligence...")
    import time
    domain_data = {
        "domain": domain, "has_ssl": url.startswith("https"),
        "server_tech": None, "response_time_ms": None,
        "page_speed_hint": None, "redirects": False,
        "app_ios": False, "app_android": False, "whatsapp_business": False,
    }
    try:
        start   = time.time()
        r       = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
        elapsed = int((time.time() - start) * 1000)
        domain_data["response_time_ms"] = elapsed
        domain_data["page_speed_hint"]  = (
            "Fast (<1s response)"   if elapsed < 1000 else
            "Medium (1-3s response)" if elapsed < 3000 else
            "Slow (>3s response)"
        )
        if len(r.history) > 0:
            domain_data["redirects"] = True
        server = r.headers.get("server","") or r.headers.get("x-powered-by","")
        if server:
            domain_data["server_tech"] = server[:50]
        domain_data["has_ssl"] = r.url.startswith("https")
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            if "apps.apple.com" in href:                          domain_data["app_ios"]           = True
            if "play.google.com" in href:                         domain_data["app_android"]        = True
            if "wa.me" in href or "whatsapp.com/send" in href:   domain_data["whatsapp_business"]  = True
    except:
        pass
    speed = domain_data.get("page_speed_hint", "Unknown")
    print(f"     ✅ Speed: {speed} | iOS: {domain_data['app_ios']} | Android: {domain_data['app_android']} | WA: {domain_data['whatsapp_business']}")
    return domain_data


# ═══════════════════════════════════════════════════
# FIX 3 — PRICING: validate domain before parsing
# ═══════════════════════════════════════════════════

def analyze_pricing(base_url):
    """FIX 3: Only parse prices from the actual target domain to avoid scraping RivalScan's own $199 price."""
    print(f"  💰 Pricing Intelligence...")
    pricing = {
        "has_pricing_page": False, "pricing_url": None,
        "pricing_model": "Unknown", "free_tier": False, "free_trial": False,
        "discount_signals": [], "price_points": [], "pricing_strategy": "Unknown",
    }
    target_domain = extract_domain(base_url)

    for path in ["/pricing","/plans","/packages","/rates","/quotes","/buy","/get-quote","/compare","/quote","/buy-online","/get-started","/start","/products"]:
        url = base_url.rstrip("/") + path
        r   = safe_get(url)
        if not r or len(r.text) < 500:
            continue

        # FIX: only parse pricing from the actual target domain — prevents self-bleed
        final_domain = extract_domain(r.url)
        if target_domain not in final_domain and final_domain not in target_domain:
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()
        if any(k in text.lower() for k in ["price","plan","package","aed","usd","$","free","month"]):
            pricing["has_pricing_page"] = True
            pricing["pricing_url"]      = url
            if "free" in text.lower():
                pricing["free_tier"] = True
            if any(k in text.lower() for k in ["trial","try free","free for"]):
                pricing["free_trial"] = True
            for pattern in [r"\d+%\s*off", r"save\s+\d+", r"discount", r"promo", r"offer"]:
                pricing["discount_signals"].extend(re.findall(pattern, text, re.I)[:3])
            for pat in [r"AED\s*[\d,]+", r"\$\s*[\d,]+", r"[\d,]+\s*AED", r"USD\s*[\d,]+", r"₹\s*[\d,]+", r"INR\s*[\d,]+"]:
                for found in re.findall(pat, text, re.I)[:5]:
                    # FIX: skip $199/USD 199 — likely RivalScan's own price bleeding in via About page scrape
                    if re.search(r"199", found) and re.search(r"USD|\$", found, re.I):
                        continue
                    pricing["price_points"].append(found)
            if pricing["free_tier"]:          pricing["pricing_strategy"] = "Freemium model"
            elif pricing["free_trial"]:       pricing["pricing_strategy"] = "Free trial acquisition"
            elif pricing["discount_signals"]: pricing["pricing_strategy"] = "Discount/promotional pricing"
            else:                              pricing["pricing_strategy"] = "Direct paid acquisition"
            break

    # Also scan homepage
    if not pricing["has_pricing_page"]:
        r2 = safe_get(base_url)
        if r2:
            text2 = BeautifulSoup(r2.text, "html.parser").get_text()
            for pat in [r"AED\s*[\d,]+", r"\$\s*[\d,]+", r"₹\s*[\d,]+"]:
                for found in re.findall(pat, text2, re.I)[:3]:
                    if re.search(r"199", found) and re.search(r"USD|\$", found, re.I):
                        continue
                    pricing["price_points"].append(found)

    print(f"     ✅ Has pricing page: {pricing['has_pricing_page']} | Model: {pricing['pricing_strategy']}")
    return pricing


# ═══════════════════════════════════════════════════
# API: HUNTER.IO
# ═══════════════════════════════════════════════════

def hunter_find_emails(domain):
    if not HUNTER_API_KEY:
        return None
    domain = re.sub(r"https?://", "", domain)
    domain = re.sub(r"^www\.", "", domain).split("/")[0].strip().lower()
    print(f"  📧 Hunter.io: {domain}")
    try:
        url    = "https://api.hunter.io/v2/domain-search"
        params = {"domain": domain, "api_key": HUNTER_API_KEY, "limit": 20}
        r      = requests.get(url, params=params, timeout=15)
        if r.status_code == 400:
            base    = ".".join(domain.split(".")[-2:])
            params["domain"] = base
            r = requests.get(url, params=params, timeout=15)
        if r.status_code not in [200]:
            print(f"     ⚠ Hunter.io error {r.status_code}")
            return None
        data       = r.json().get("data", {})
        emails_raw = data.get("emails", [])
        result = {
            "found": len(emails_raw) > 0, "total": len(emails_raw),
            "organization": data.get("organization",""),
            "emails": [], "ceo_email": None, "sales_email": None,
            "support_email": None, "press_email": None, "all_named_contacts": [],
        }
        for e in emails_raw:
            email_obj = {
                "email": e.get("value",""), "type": e.get("type","generic"),
                "confidence": e.get("confidence", 0),
                "verified": e.get("verification",{}).get("status","") == "valid",
                "first_name": e.get("first_name",""), "last_name": e.get("last_name",""),
                "position": e.get("position",""),
            }
            result["emails"].append(email_obj)
            email_addr = email_obj["email"]
            position   = email_obj["position"].lower()
            email_low  = email_addr.lower()
            if email_obj["first_name"]:
                name = f"{email_obj['first_name']} {email_obj['last_name']}".strip()
                result["all_named_contacts"].append(f"{name} ({email_obj['position'] or 'Staff'}) — {email_addr}")
            if any(k in position for k in ["ceo","chief executive","founder","managing director"]):
                result["ceo_email"] = email_addr
            elif any(k in position for k in ["sales","business dev","account","revenue"]):
                result["sales_email"] = result["sales_email"] or email_addr
            elif any(k in position for k in ["support","service","help","care"]):
                result["support_email"] = email_addr
            elif any(k in position for k in ["press","pr","media","comms","marketing"]):
                result["press_email"] = email_addr
            elif any(k in email_low for k in ["ask","hello","info","contact","hi"]):
                result["sales_email"] = result["sales_email"] or email_addr
        if not result["sales_email"] and result["emails"]:
            result["sales_email"] = result["emails"][0]["email"]
        print(f"     ✅ {result['total']} emails | CEO: {result['ceo_email'] or 'Not found'}")
        return result
    except Exception as e:
        print(f"     ⚠ Hunter.io failed: {e}")
        return None


# ═══════════════════════════════════════════════════
# API: PAGESPEED
# ═══════════════════════════════════════════════════

def get_pagespeed(url):
    if not GOOGLE_API_KEY:
        return None
    print(f"  ⚡ PageSpeed API...")
    result = {"desktop_score": None, "mobile_score": None, "lcp": None, "cls": None, "fcp": None, "ttfb": None, "opportunities": []}
    for strategy in ["desktop", "mobile"]:
        try:
            params = {"url": url, "key": GOOGLE_API_KEY, "strategy": strategy, "category": "performance"}
            r      = requests.get(PAGESPEED_URL, params=params, timeout=20)
            if not r or r.status_code != 200:
                continue
            data   = r.json()
            cats   = data.get("lighthouseResult",{}).get("categories",{})
            score  = int(cats.get("performance",{}).get("score",0) * 100)
            if strategy == "desktop":
                result["desktop_score"] = score
            else:
                result["mobile_score"] = score
                audits = data.get("lighthouseResult",{}).get("audits",{})
                result["lcp"]  = audits.get("largest-contentful-paint",{}).get("displayValue","")
                result["cls"]  = audits.get("cumulative-layout-shift",{}).get("displayValue","")
                result["fcp"]  = audits.get("first-contentful-paint",{}).get("displayValue","")
                result["ttfb"] = audits.get("server-response-time",{}).get("displayValue","")
                for key, audit in audits.items():
                    if audit.get("score",1) is not None and audit.get("score",1) < 0.9:
                        savings = audit.get("details",{}).get("overallSavingsMs",0)
                        if savings > 200:
                            result["opportunities"].append(f"{audit.get('title','')}: save {int(savings)}ms")
        except:
            pass
    print(f"     ✅ Desktop: {result['desktop_score'] or '?'}/100 | Mobile: {result['mobile_score'] or '?'}/100")
    return result


# ═══════════════════════════════════════════════════
# API: YOUTUBE DATA
# ═══════════════════════════════════════════════════

def get_youtube_stats(channel_url=None, company_name=None):
    if not GOOGLE_API_KEY:
        return None
    print(f"  📺 YouTube Data API...")
    result = {"found": False, "channel_id": None, "channel_name": None, "subscribers": None, "total_views": None, "video_count": None, "recent_videos": [], "upload_frequency": "Unknown"}
    try:
        params = {"part": "snippet", "q": company_name or "", "type": "channel", "maxResults": 3, "key": GOOGLE_API_KEY}
        r      = requests.get(f"{YOUTUBE_API_URL}/search", params=params, timeout=10)
        if not r or r.status_code != 200:
            return None
        items = r.json().get("items", [])
        if not items:
            return None
        channel_id = None
        for item in items:
            ch_title = item.get("snippet",{}).get("channelTitle","").lower()
            if company_name and company_name.lower()[:5] in ch_title:
                channel_id = item["id"]["channelId"]
                break
        if not channel_id:
            channel_id = items[0]["id"]["channelId"]
        params2 = {"part": "statistics,snippet,contentDetails", "id": channel_id, "key": GOOGLE_API_KEY}
        r2      = requests.get(f"{YOUTUBE_API_URL}/channels", params=params2, timeout=10)
        if not r2 or r2.status_code != 200:
            return None
        ch_data = r2.json().get("items",[{}])[0]
        stats   = ch_data.get("statistics",{})
        snippet = ch_data.get("snippet",{})
        result.update({
            "found": True, "channel_id": channel_id,
            "channel_name": snippet.get("title",""),
            "subscribers":  stats.get("subscriberCount","Hidden"),
            "total_views":  stats.get("viewCount",""),
            "video_count":  stats.get("videoCount",""),
        })
        for key in ["subscribers","total_views","video_count"]:
            val = result[key]
            if val and str(val).isdigit():
                num = int(val)
                result[key] = f"{num/1_000_000:.1f}M" if num >= 1_000_000 else f"{num/1_000:.1f}K" if num >= 1_000 else str(num)
        uploads_id = ch_data.get("contentDetails",{}).get("relatedPlaylists",{}).get("uploads","")
        if uploads_id:
            params3 = {"part": "snippet,contentDetails", "playlistId": uploads_id, "maxResults": 10, "key": GOOGLE_API_KEY}
            r3      = requests.get(f"{YOUTUBE_API_URL}/playlistItems", params=params3, timeout=10)
            if r3 and r3.status_code == 200:
                for item in r3.json().get("items",[]):
                    snip = item.get("snippet",{})
                    result["recent_videos"].append({"title": snip.get("title","")[:80], "published": snip.get("publishedAt","")[:10]})
        count = len(result["recent_videos"])
        result["upload_frequency"] = "Very Active" if count >= 8 else "Active" if count >= 4 else "Moderate" if count >= 2 else "Inactive"
        print(f"     ✅ Subscribers: {result['subscribers'] or '?'} | Videos: {result['video_count'] or '?'}")
        return result
    except Exception as e:
        print(f"     ⚠ YouTube API failed: {e}")
        return None


# ═══════════════════════════════════════════════════
# API: NEWSAPI
# ═══════════════════════════════════════════════════

def newsapi_search(company_name, domain):
    if not NEWS_API_KEY:
        return None
    country_code = detect_country(domain)
    country_name = {"ae":"UAE","uk":"UK","au":"Australia","in":"India","sg":"Singapore","sa":"Saudi Arabia","qa":"Qatar","bh":"Bahrain","kw":"Kuwait","eg":"Egypt"}.get(country_code,"")
    queries = [f'"{domain}"', f'"{company_name}" {country_name}' if country_name else f'"{company_name}"']
    print(f"  📰 NewsAPI: {domain}...")
    result = {"articles": [], "total": 0, "funding_mentioned": False, "acquisition_mentioned": False, "partnership_mentioned": False, "award_mentioned": False, "negative_mentioned": False, "signals": []}
    try:
        for query in queries:
            params = {"q": query, "sortBy": "publishedAt", "pageSize": 10, "language": "en", "apiKey": NEWS_API_KEY}
            r      = requests.get("https://newsapi.org/v2/everything", params=params, timeout=12)
            if not r or r.status_code not in [200]:
                continue
            articles = r.json().get("articles",[])
            for art in articles:
                title       = (art.get("title","") or "").strip()
                description = (art.get("description","") or "").strip()
                source_name = art.get("source",{}).get("name","")
                published   = art.get("publishedAt","")[:10]
                if not title or "[Removed]" in title:
                    continue
                combined    = (title + " " + description).lower()
                domain_base = domain.split(".")[0].lower()
                is_relevant = (domain.lower() in combined or domain_base in combined or (country_name and country_name.lower() in combined) or not country_name)
                if not is_relevant:
                    continue
                article_obj = {"title": title[:150], "description": description[:200], "source": source_name, "date": published, "url": art.get("url","")}
                if title not in [a["title"] for a in result["articles"]]:
                    result["articles"].append(article_obj)
                tl = combined
                if any(k in tl for k in ["raises","funding","series","million","billion"]): result["funding_mentioned"]     = True
                if any(k in tl for k in ["acquires","acquired","merger","acquisition"]):    result["acquisition_mentioned"] = True
                if any(k in tl for k in ["partner","partnership","collaboration"]):         result["partnership_mentioned"] = True
                if any(k in tl for k in ["award","wins","winner","best","ranked"]):         result["award_mentioned"]       = True
                if any(k in tl for k in ["layoff","lawsuit","fraud","scandal","fine"]):     result["negative_mentioned"]    = True
            if len(result["articles"]) >= 4:
                result["total"] = r.json().get("totalResults", len(result["articles"]))
                break
        result["total"] = result["total"] or len(result["articles"])
        if result["funding_mentioned"]:     result["signals"].append("Funding Activity")
        if result["acquisition_mentioned"]: result["signals"].append("M&A Activity")
        if result["partnership_mentioned"]: result["signals"].append("Partnership Announced")
        if result["award_mentioned"]:       result["signals"].append("Award/Recognition")
        if result["negative_mentioned"]:    result["signals"].append("Negative Press Detected")
        print(f"     ✅ {len(result['articles'])} articles | Signals: {', '.join(result['signals']) or 'None'}")
        return result
    except Exception as e:
        print(f"     ⚠ NewsAPI failed: {e}")
        return None


# ═══════════════════════════════════════════════════
# API: WHOIS/RDAP
# ═══════════════════════════════════════════════════

def get_domain_age(domain):
    print(f"  🌍 WHOIS/RDAP...")
    result = {"registered": None, "expires": None, "registrar": None, "age_years": None, "nameservers": [], "trust_signal": "Unknown"}
    try:
        r = requests.get(f"https://rdap.org/domain/{domain}", timeout=8, headers={"Accept": "application/json"})
        if r and r.status_code == 200:
            data = r.json()
            for event in data.get("events",[]):
                action = event.get("eventAction","")
                date   = event.get("eventDate","")[:10]
                if "registration" in action.lower(): result["registered"] = date
                elif "expir" in action.lower():       result["expires"]    = date
            for entity in data.get("entities",[]):
                if "registrar" in entity.get("roles",[]):
                    vcard = entity.get("vcardArray",["",[[]]]) [1]
                    for field in vcard:
                        if field[0] == "fn":
                            result["registrar"] = field[3][:50]
                            break
            for ns in data.get("nameservers",[])[:3]:
                result["nameservers"].append(ns.get("ldhName",""))
            if result["registered"]:
                from datetime import date as date_obj
                try:
                    reg_year  = int(result["registered"][:4])
                    age_years = date_obj.today().year - reg_year
                    result["age_years"] = age_years
                    result["trust_signal"] = (
                        f"Established domain ({age_years} years old)" if age_years >= 10 else
                        f"Mature domain ({age_years} years old)"      if age_years >= 5  else
                        f"Newer domain ({age_years} years old)"       if age_years >= 2  else
                        f"Very new domain"
                    )
                except:
                    pass
    except:
        pass
    age = f"{result['age_years']} years" if result["age_years"] else "Unknown"
    print(f"     ✅ Registered: {result['registered'] or 'Unknown'} | Age: {age}")
    return result


# ═══════════════════════════════════════════════════
# FACEBOOK AD LIBRARY
# ═══════════════════════════════════════════════════

def scrape_facebook_ads(company_name, domain, country="AE"):
    print(f"  📘 Facebook Ad Library: {company_name}...")
    result = {"found": False, "total_ads": 0, "active_ads": [], "messaging_angles": [], "ctas_used": [], "spend_estimate": "Unknown", "ad_library_url": None, "note": ""}
    domain_base = domain.split(".")[0].lower()
    clean_q     = requests.utils.quote(company_name)
    result["ad_library_url"] = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country={country}&q={clean_q}&search_type=keyword_unordered"
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9", "Referer": "https://www.google.com/",
    }
    for term in [company_name, domain_base, domain]:
        try:
            url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country={country}&q={requests.utils.quote(term)}&search_type=keyword_unordered"
            r   = requests.get(url, headers=browser_headers, timeout=15)
            if not r or r.status_code != 200 or len(r.text) < 3000:
                continue
            text     = r.text
            ad_texts = []
            for pat in [r'"body"\s*:\s*"([^"]{20,300})"', r'"message"\s*:\s*"([^"]{20,300})"', r'"ad_creative_body"\s*:\s*"([^"]{20,300})"']:
                ad_texts.extend(re.findall(pat, text))
            m = re.search(r'"total_count"\s*:\s*(\d+)', text)
            if m:
                result["total_ads"] = int(m.group(1))
            if ad_texts or result["total_ads"] > 0:
                result["found"] = True
                result["note"]  = "Facebook Ad Library"
                seen = set()
                for t in ad_texts[:15]:
                    clean = t.replace("\n"," ").replace("\'","'").strip()
                    if clean not in seen and len(clean) > 15:
                        seen.add(clean)
                        result["active_ads"].append({"text": clean[:250]})
                ctas = re.findall(r'"cta_type"\s*:\s*"([^"]+)"', text)
                result["ctas_used"] = list(set([c.replace("_"," ").title() for c in ctas if c and "NO_BUTTON" not in c]))[:5]
                break
        except:
            continue

    # mbasic fallback
    if not result["found"]:
        try:
            mb_url = f"https://mbasic.facebook.com/ads/library/?active_status=active&country={country}&q={clean_q}"
            r2     = requests.get(mb_url, headers={"User-Agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15","Accept":"text/html,application/xhtml+xml","Referer":"https://www.google.com/"}, timeout=12)
            if r2 and r2.status_code == 200 and len(r2.text) > 500:
                soup  = BeautifulSoup(r2.text, "html.parser")
                text2 = r2.text
                if any(k in text2.lower() for k in ["active ad","ad id","sponsored","advertisement"]):
                    result["found"] = True
                    result["note"]  = "mbasic Facebook"
                    for div in soup.find_all(["div","td","p"])[:30]:
                        t = div.get_text(strip=True)
                        if 25 < len(t) < 300 and not any(k in t.lower() for k in ["facebook","privacy","cookie","terms","login"]):
                            result["active_ads"].append({"text": t[:250]})
                            if len(result["active_ads"]) >= 8: break
        except:
            pass

    if result["active_ads"]:
        result["total_ads"] = result["total_ads"] or len(result["active_ads"])
        all_text = " ".join([a["text"] for a in result["active_ads"]]).lower()
        theme_map = {
            "Price / Savings":     ["save","cheap","lowest","affordable","compare","discount","offer"],
            "Speed / Convenience": ["quick","fast","instant","seconds","easy","simple","online"],
            "Trust / Experience":  ["year","trusted","expert","certified","award","guarantee"],
            "Free Offer":          ["free","no cost","trial","demo"],
            "Urgency":             ["today","now","limited","hurry","last chance"],
        }
        for theme, kws in theme_map.items():
            if any(kw in all_text for kw in kws):
                result["messaging_angles"].append(theme)

    total = result["total_ads"]
    result["spend_estimate"] = (
        "Very High (20+ ads)" if total >= 20 else "High (10-19 ads)" if total >= 10 else
        "Medium (5-9 ads)"    if total >= 5  else "Low (1-4 ads)"    if total >= 1  else
        "None detected"
    )
    print(f"     ✅ FB Ads: {total} | Spend: {result['spend_estimate']}")
    return result


def scrape_google_ads(company_name, domain, country="AE"):
    print(f"  🔍 Google Ads Transparency: {company_name}...")
    result = {"found": False, "ads": [], "total": 0, "transparency_url": None}
    result["transparency_url"] = f"https://adstransparency.google.com/?region=anywhere&hl=en&advertiserName={requests.utils.quote(company_name)}"
    try:
        r = requests.get(f"https://www.google.com/search?q={requests.utils.quote(company_name)}+ad", headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}, timeout=10)
        if r:
            soup = BeautifulSoup(r.text, "html.parser")
            for el in soup.find_all(["div","span"], attrs={"aria-label": re.compile(r"[Aa]d", re.I)}):
                t = el.get_text(strip=True)
                if 10 < len(t) < 200:
                    result["ads"].append({"text": t[:200]})
                    result["found"] = True
            result["total"] = len(result["ads"])
    except:
        pass
    print(f"     ✅ Google Ads: {result['total']} found | Active: {'Yes' if result['found'] else 'Not detected'}")
    return result


# ═══════════════════════════════════════════════════
# GOOGLE BUSINESS INTELLIGENCE
# ═══════════════════════════════════════════════════

def google_business_intelligence(company_name, domain, country="UAE"):
    print(f"  🔍 Google Business Intel: {company_name}...")
    intel = {
        "google_rating": None, "google_reviews": None, "google_maps": False,
        "knowledge_panel": False, "business_type": None, "address": None,
        "phone_from_google": None, "people_also_ask": [], "related_searches": [],
        "competitor_mentions": [], "serp_features": [], "google_snippets": [],
    }
    for query in [f"{company_name} {country} review", f"{company_name} {country}"]:
        try:
            r = requests.get(
                f"https://www.google.com/search?q={requests.utils.quote(query)}&hl=en&gl=ae",
                headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36","Accept-Language":"en-US,en;q=0.9"},
                timeout=10
            )
            if not r:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            text = r.text
            m = re.search(r'"ratingValue"\s*:\s*"?([\d.]+)"?', text)
            if m and not intel["google_rating"]: intel["google_rating"] = m.group(1)
            m = re.search(r'"reviewCount"\s*:\s*"?(\d+)"?', text)
            if m and not intel["google_reviews"]: intel["google_reviews"] = m.group(1)
            if "kp-header" in text or "knowledge-panel" in text or "kno-result" in text: intel["knowledge_panel"] = True
            if "maps.google" in text or "google.com/maps" in text: intel["google_maps"] = True
            paa = re.findall(r'class="[^"]*related-question[^"]*"[^>]*>([^<]{10,120})', text)
            for q in paa[:5]:
                cq = re.sub(r'<[^>]+>', '', q).strip()
                if len(cq) > 10 and cq not in intel["people_also_ask"]: intel["people_also_ask"].append(cq)
            m2 = re.search(r'"streetAddress"\s*:\s*"([^"]+)"', text)
            if m2 and not intel["address"]: intel["address"] = m2.group(1)[:100]
            m3 = re.search(r'tel:([+\d\s\-()]{7,20})', text)
            if m3 and not intel["phone_from_google"]: intel["phone_from_google"] = m3.group(1).strip()
            if intel["google_rating"] or intel["knowledge_panel"]: break
        except:
            continue
    print(f"     ✅ Rating: {intel['google_rating'] or 'Not found'} | Maps: {intel['google_maps']} | KP: {intel['knowledge_panel']}")
    return intel


# ═══════════════════════════════════════════════════
# APP STORE
# ═══════════════════════════════════════════════════

def scrape_app_store(company_name, domain):
    print(f"  📱 App Store: {company_name}...")
    result = {
        "ios_found": False, "android_found": False,
        "ios_rating": None, "ios_reviews": None, "ios_version": None, "ios_updated": None, "ios_size": None,
        "android_rating": None, "android_reviews": None, "android_installs": None,
        "recent_reviews": [], "top_complaints": [], "top_praises": [], "feature_mentions": [],
        "overall_sentiment": "Unknown", "ios_url": None, "android_url": None,
    }
    clean = company_name.lower().replace(" ","").replace(".com","").replace(".ae","")
    try:
        r = requests.get(f"https://itunes.apple.com/search?term={requests.utils.quote(company_name)}&entity=software&limit=5", timeout=10)
        if r and r.status_code == 200:
            results = r.json().get("results",[])
            app     = next((a for a in results if clean[:5] in a.get("trackName","").lower()), results[0] if results else None)
            if app:
                result["ios_found"]   = True
                result["ios_rating"]  = round(float(app.get("averageUserRating",0)), 1) if app.get("averageUserRating") else None
                result["ios_reviews"] = app.get("userRatingCount")
                result["ios_version"] = app.get("version")
                result["ios_updated"] = app.get("currentVersionReleaseDate","")[:10]
                result["ios_url"]     = app.get("trackViewUrl")
                if app.get("fileSizeBytes"):
                    result["ios_size"] = f"{round(int(app['fileSizeBytes'])/(1024*1024),1)}MB"
    except:
        pass
    try:
        r2 = requests.get(f"https://play.google.com/store/search?q={requests.utils.quote(company_name)}&c=apps", headers=HEADERS, timeout=10)
        if r2 and r2.status_code == 200:
            app_ids = re.findall(r'id=([a-z][a-z0-9._]+)', r2.text)
            app_id  = next((aid for aid in app_ids if clean[:4] in aid.lower()), app_ids[0] if app_ids else None)
            if app_id:
                r3 = requests.get(f"https://play.google.com/store/apps/details?id={app_id}&hl=en", headers=HEADERS, timeout=12)
                if r3 and r3.status_code == 200:
                    t3 = r3.text
                    m  = re.search(r'"starRating":\s*"([\d.]+)"', t3)
                    if m: result["android_rating"] = float(m.group(1)); result["android_found"] = True
                    m  = re.search(r'"numDownloads":\s*"([^"]+)"', t3)
                    if not m: m = re.search(r'([\d,]+\+?)\s*(?:installs?|downloads?)', t3, re.I)
                    if m: result["android_installs"] = m.group(1); result["android_found"] = True
                    result["android_url"] = f"https://play.google.com/store/apps/details?id={app_id}"
    except:
        pass
    # Scrape iOS reviews
    try:
        if result["ios_url"]:
            app_id_m = re.search(r'/id(\d+)', result["ios_url"])
            if app_id_m:
                rv = requests.get(f"https://itunes.apple.com/rss/customerreviews/id={app_id_m.group(1)}/sortby=mostrecent/json", timeout=10)
                if rv and rv.status_code == 200:
                    for entry in rv.json().get("feed",{}).get("entry",[])[1:16]:
                        rating = entry.get("im:rating",{}).get("label","")
                        body   = entry.get("content",{}).get("label","")
                        title  = entry.get("title",{}).get("label","")
                        if body:
                            result["recent_reviews"].append({"rating": int(rating) if rating.isdigit() else 3, "title": title[:80], "body": body[:200], "platform": "iOS"})
    except:
        pass
    if result["recent_reviews"]:
        all_text = " ".join([r["body"].lower() for r in result["recent_reviews"]])
        negative = [r for r in result["recent_reviews"] if r["rating"] <= 2]
        positive = [r for r in result["recent_reviews"] if r["rating"] >= 4]
        ck = Counter()
        for rev in negative:
            for word in rev["body"].lower().split():
                if len(word) > 4 and word not in {"this","that","with","have","from","they","their","when","what","been"}: ck[word] += 1
        result["top_complaints"] = [w for w,c in ck.most_common(8)]
        pk = Counter()
        for rev in positive:
            for word in rev["body"].lower().split():
                if len(word) > 4 and word not in {"this","that","with","have","from","they","their","when","what","been"}: pk[word] += 1
        result["top_praises"] = [w for w,c in pk.most_common(8)]
        for feat in ["delivery","payment","tracking","support","discount","subscription","gold","one","pro","premium","speed","crash","slow","fast","refund","customer service","order","restaurant","menu"]:
            if feat in all_text: result["feature_mentions"].append(feat)
        total     = len(result["recent_reviews"])
        pos_ratio = len(positive) / total if total > 0 else 0
        result["overall_sentiment"] = (
            "Very Positive" if pos_ratio >= 0.8 else "Positive" if pos_ratio >= 0.6 else
            "Mixed"         if pos_ratio >= 0.4 else "Negative" if pos_ratio >= 0.2 else "Very Negative"
        )
    ios_str = f"iOS: {result['ios_rating']}/5 ({result['ios_reviews']:,} ratings)" if result["ios_found"] and result["ios_reviews"] else ("iOS: Found" if result["ios_found"] else "iOS: Not found")
    and_str = f"Android: {result['android_rating']}/5" if result["android_found"] else "Android: Not found"
    print(f"     ✅ {ios_str} | {and_str} | Reviews: {len(result['recent_reviews'])}")
    return result


# ═══════════════════════════════════════════════════
# REDDIT SENTIMENT
# ═══════════════════════════════════════════════════

def scrape_reddit_sentiment(company_name, domain):
    print(f"  💬 Reddit: {company_name}...")
    result = {
        "found": False, "total_mentions": 0, "positive_mentions": 0,
        "negative_mentions": 0, "neutral_mentions": 0, "sentiment_score": 0,
        "top_subreddits": [], "recent_posts": [], "top_complaints": [],
        "top_praises": [], "trending_topics": [],
    }
    clean   = company_name.lower().replace(" ","").replace(".com","").replace(".ae","")
    country = detect_country(domain)
    industry_subs = {
        "food":      ["india","bangalore","mumbai","delhi","hyderabad","pune"],
        "insurance": ["dubai","UAE","personalfinance","insurance"],
        "fintech":   ["india","personalfinance","investing"],
        "saas":      ["saas","startups","entrepreneur"],
    }
    detected = "general"
    for ind, kws in {"food":["food","swiggy","zomato","delivery","restaurant"],"insurance":["insurance","policy","insure"]}.items():
        if any(k in company_name.lower() or k in domain.lower() for k in kws):
            detected = ind
            break
    search_subs = ["all"] + industry_subs.get(detected, [])

    for sub in search_subs[:5]:
        try:
            url = f"https://www.reddit.com/r/{sub}/search.json?q={requests.utils.quote(company_name)}&sort=new&limit=15&restrict_sr=false"
            r   = requests.get(url, headers={"User-Agent": "RivalScan Intelligence Bot 1.0"}, timeout=12)
            if not r or r.status_code != 200:
                continue
            for post in r.json().get("data",{}).get("children",[]):
                pd       = post.get("data",{})
                title    = pd.get("title","")
                body     = pd.get("selftext","")
                score    = pd.get("score",0)
                sub_name = pd.get("subreddit","")
                combined = (title + " " + body).lower()
                if clean[:5] not in combined and company_name.lower()[:5] not in combined:
                    continue
                result["found"]          = True
                result["total_mentions"] += 1
                neg_words = {"bad","terrible","worst","hate","awful","scam","fraud","cheat","waste","disgusting","horrible","pathetic","refund","complaint","issue","problem","failed","broken"}
                pos_words = {"great","love","excellent","amazing","best","awesome","fantastic","perfect","superb","wonderful","recommend","satisfied","happy","fast","quick","easy","reliable"}
                neg_count = sum(1 for w in neg_words if w in combined)
                pos_count = sum(1 for w in pos_words if w in combined)
                if neg_count > pos_count:   result["negative_mentions"] += 1; sentiment = "negative"
                elif pos_count > neg_count: result["positive_mentions"] += 1; sentiment = "positive"
                else:                       result["neutral_mentions"]  += 1; sentiment = "neutral"
                result["recent_posts"].append({"title": title[:120], "sentiment": sentiment, "score": score, "subreddit": sub_name, "snippet": body[:150] if body else ""})
                if sub_name not in result["top_subreddits"]: result["top_subreddits"].append(sub_name)
        except:
            continue

    total = result["total_mentions"]
    if total > 0:
        result["sentiment_score"] = round((result["positive_mentions"] - result["negative_mentions"]) / total * 100)
    if result["recent_posts"]:
        all_titles = " ".join([p["title"] for p in result["recent_posts"]]).lower()
        stop = {"the","a","an","and","or","but","in","on","at","to","for","of","with","by","from","is","are","was","be","this","that","reddit",company_name.lower()[:5]}
        words = [w for w in all_titles.split() if len(w) > 4 and w not in stop]
        result["trending_topics"] = [w for w, _ in Counter(words).most_common(8)]

    sentiment_s = f"+{result['sentiment_score']}%" if result["sentiment_score"] >= 0 else f"{result['sentiment_score']}%"
    print(f"     ✅ {total} mentions | Sentiment: {sentiment_s} | Subreddits: {', '.join(result['top_subreddits'][:3]) or 'None'}")
    return result


# ═══════════════════════════════════════════════════
# FIX 3 (continued) — DEEP PRICING with domain validation
# ═══════════════════════════════════════════════════

def deep_pricing_intelligence(base_url, company_name):
    """FIX 3: Validates all scraped URLs are on target domain before parsing prices."""
    print(f"  💰 Deep Pricing: {company_name}...")
    result = {
        "has_subscription": False, "subscription_plans": [], "subscription_price": None,
        "delivery_fees": [], "commission_rate": None, "free_tier": False,
        "free_tier_details": None, "discount_codes": [], "current_offers": [],
        "price_points": [], "pricing_model": "Unknown", "pricing_strategy": "Unknown",
        "membership_benefits": [], "currency": "Unknown",
    }
    country = detect_country(base_url.split("/")[2].replace("www.",""))
    result["currency"] = {"ae":"AED","in":"INR","uk":"GBP","au":"AUD","sg":"SGD","us":"USD"}.get(country,"USD")
    target_domain = extract_domain(base_url)

    pricing_paths = [
        "/pricing","/plans","/subscription","/gold","/one","/pro","/premium","/membership",
        "/subscribe","/swiggyone","/zomatogold","/swiggy-one","/delivery-fee","/charges",
        "/fees","/offers","/coupons","/deals","/promo","/business","/for-business",
        "/partner","/restaurant-partner","/about-charges","/how-it-works",
    ]
    all_pricing_text = ""
    for path in pricing_paths:
        url = base_url.rstrip("/") + path
        r   = safe_get(url)
        if not r or len(r.text) < 300:
            continue
        # FIX: validate we're still on the target domain
        final_domain = extract_domain(r.url)
        if target_domain not in final_domain and final_domain not in target_domain:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style","nav","footer"]): tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        all_pricing_text += " " + text

        for pat in [r"(?:plan|tier|package)\s*[:\-]?\s*([^\n\r]{5,50})", r"(\w+\s+(?:plan|tier|membership|subscription))"]:
            for plan in re.findall(pat, text, re.I)[:5]:
                if len(plan) > 3 and plan not in result["subscription_plans"]:
                    result["subscription_plans"].append(plan.strip()[:60])
                    result["has_subscription"] = True

        for pat in [rf"(?:AED|INR|RS|Rs\.?|₹|\$|£)\s*([\d,]+(?:\.\d{{2}})?)", r"([\d,]+(?:\.\d{2})?)\s*(?:AED|INR|per month|/month|/mo|monthly)"]:
            for price in re.findall(pat, text, re.I)[:8]:
                p = price.replace(",","")
                # FIX: skip $199 which is RivalScan's own price
                if p.replace(".","").isdigit() and p not in ["199","199.00"]:
                    result["price_points"].append(f"{result['currency']} {price}")

        for pat in [r"delivery\s+(?:fee|charge|cost)\s*(?:of|:)?\s*(?:AED|INR|₹|Rs)?\s*([\d,]+)", r"(?:free|no)\s+delivery\s+(?:on|above|over)\s+(?:AED|INR|₹|Rs)?\s*([\d,]+)"]:
            m = re.search(pat, text, re.I)
            if m: result["delivery_fees"].append(m.group(0)[:80])

        m = re.search(r"(\d+(?:\.\d+)?)\s*%\s*(?:commission|take rate|platform fee)", text, re.I)
        if m: result["commission_rate"] = f"{m.group(1)}%"

        if any(k in text.lower() for k in ["free delivery","free trial","free plan","no minimum"]):
            result["free_tier"] = True
            fm = re.search(r"free\s+(?:delivery|trial|plan)\s+(?:on|above|for)?\s*([^\n\r.]{5,60})", text, re.I)
            if fm: result["free_tier_details"] = fm.group(0)[:100]

        for pat in [r"(\d+)\s*%\s*(?:off|discount|cashback)", r"(?:flat|extra|additional)\s+(\d+)\s*%\s*off"]:
            for o in re.findall(pat, text, re.I)[:3]:
                offer_str = f"{o}% off"
                if offer_str not in result["current_offers"]: result["current_offers"].append(offer_str)

        for pat in [r"(?:free|unlimited|priority|exclusive)\s+(?:delivery|support|access|returns?)[^\n\r.]{0,50}"]:
            for b in re.findall(pat, text, re.I)[:5]:
                if b.strip() not in result["membership_benefits"]: result["membership_benefits"].append(b.strip()[:80])

    text_lower = all_pricing_text.lower()
    if result["has_subscription"]:    result["pricing_model"] = "Subscription + Transaction"; result["pricing_strategy"] = "Recurring revenue model"
    elif "commission" in text_lower:  result["pricing_model"] = "Commission-based"; result["pricing_strategy"] = "Marketplace model"
    elif result["free_tier"]:         result["pricing_model"] = "Freemium"; result["pricing_strategy"] = "Free acquisition → paid upsell"
    elif result["price_points"]:      result["pricing_model"] = "Fixed pricing"; result["pricing_strategy"] = "Direct purchase model"

    result["price_points"]       = list(set(result["price_points"]))[:8]
    result["current_offers"]     = list(set(result["current_offers"]))[:5]
    result["membership_benefits"] = result["membership_benefits"][:6]

    subs = "Has subscription" if result["has_subscription"] else "No subscription"
    print(f"     ✅ {result['pricing_model']} | {subs}")
    return result


# ═══════════════════════════════════════════════════
# GEOGRAPHIC EXPANSION
# ═══════════════════════════════════════════════════

def analyze_geographic_expansion(base_url, company_name, domain):
    print(f"  🗺️  Geographic: {company_name}...")
    result = {
        "cities_detected": [], "countries_detected": [], "location_pages": [],
        "total_locations": 0, "expansion_signals": [],
        "primary_market": "Unknown", "geographic_spread": "Unknown",
    }
    INDIA_CITIES  = ["mumbai","delhi","bangalore","bengaluru","hyderabad","chennai","pune","kolkata","ahmedabad","surat","jaipur","lucknow","kanpur","nagpur","indore","bhopal","coimbatore","vadodara","goa","chandigarh","kochi","visakhapatnam","nashik","noida","gurugram","gurgaon","navi mumbai"]
    UAE_CITIES    = ["dubai","abu dhabi","sharjah","ajman","ras al khaimah","fujairah","umm al quwain","al ain","bur dubai","deira","marina","jlt","business bay","downtown","jumeirah"]
    GLOBAL_CITIES = ["london","new york","singapore","sydney","toronto","paris","berlin","amsterdam","hong kong","tokyo"]
    ALL_CITIES    = INDIA_CITIES + UAE_CITIES + GLOBAL_CITIES

    pages_to_scan = [base_url, base_url.rstrip("/")+"/about", base_url.rstrip("/")+"/cities", base_url.rstrip("/")+"/coverage"]
    all_text = ""
    for page_url in pages_to_scan:
        r = safe_get(page_url)
        if not r or len(r.text) < 200: continue
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style"]): tag.decompose()
        all_text += " " + soup.get_text(separator=" ", strip=True).lower()

    for city in ALL_CITIES:
        if city in all_text and city.title() not in result["cities_detected"]:
            result["cities_detected"].append(city.title())

    country_kws = {
        "India":     ["india","indian","₹","inr"],
        "UAE":       ["uae","dubai","abu dhabi","emirates","aed"],
        "Singapore": ["singapore","sgd"],
        "UK":        ["united kingdom","london","gbp"],
        "Australia": ["australia","sydney","aud"],
    }
    for country, kws in country_kws.items():
        if any(k in all_text for k in kws) and country not in result["countries_detected"]:
            result["countries_detected"].append(country)

    result["total_locations"] = len(result["cities_detected"])
    count = result["total_locations"]
    result["geographic_spread"] = (
        "Global (20+ cities)"     if count >= 20 else "National (10-19 cities)" if count >= 10 else
        "Multi-city (5-9 cities)" if count >= 5  else "Regional (2-4 cities)"   if count >= 2  else
        "Single market"           if count == 1  else "Not detected"
    )
    if result["countries_detected"]:
        result["primary_market"] = result["countries_detected"][0]
    cities_str = ", ".join(result["cities_detected"][:6]) or "None detected"
    print(f"     ✅ {result['total_locations']} cities | {result['geographic_spread']} | Markets: {', '.join(result['countries_detected'][:3]) or 'Unknown'}")
    return result


# ═══════════════════════════════════════════════════
# PRODUCT FEATURES
# ═══════════════════════════════════════════════════

def analyze_product_features(base_url, company_name, domain):
    print(f"  🔧 Product Features: {company_name}...")
    result = {
        "recent_features": [], "feature_categories": [], "product_areas": [],
        "tech_signals": [], "roadmap_hints": [], "nav_features": [],
        "unique_features": [], "changelog_found": False, "changelog_url": None,
    }
    all_feature_text = ""
    for path in ["/changelog","/whats-new","/new","/updates","/release-notes","/features","/announcements"]:
        url = base_url.rstrip("/") + path
        r   = safe_get(url)
        if not r or len(r.text) < 300: continue
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style"]): tag.decompose()
        all_feature_text += " " + soup.get_text(separator=" ", strip=True)
        result["changelog_found"] = True
        result["changelog_url"]   = url
        for h in soup.find_all(["h1","h2","h3","h4"])[:20]:
            t = h.get_text(strip=True)
            if 3 < len(t) < 80 and t not in result["recent_features"]:
                result["recent_features"].append(t)
        break

    r_home = safe_get(base_url)
    if r_home:
        soup_h = BeautifulSoup(r_home.text, "html.parser")
        for tag in soup_h(["script","style"]): tag.decompose()
        home_text = soup_h.get_text(separator=" ", strip=True)
        all_feature_text += " " + home_text
        nav = soup_h.find("nav")
        if nav:
            for a in nav.find_all("a")[:20]:
                t = a.get_text(strip=True)
                if t and 2 < len(t) < 40: result["nav_features"].append(t)

    feature_map = {
        "Food Delivery":      ["food delivery","order food","restaurant delivery"],
        "Grocery/Instamart":  ["grocery","instamart","instant delivery","10 min"],
        "Dine-out":           ["dine out","dine-out","table booking","reserve"],
        "Subscription/Gold":  ["subscription","gold","one plan","premium membership"],
        "Live Tracking":      ["live track","real-time track","track order"],
        "Scheduled Delivery": ["schedule","scheduled delivery","advance order"],
        "B2B/Corporate":      ["corporate","enterprise","office","b2b","bulk order"],
        "Loyalty/Rewards":    ["loyalty","reward","points","cashback","coins"],
        "AI/Personalization": ["personali","recomm","ai","machine learning","smart"],
        "Hyperlocal":         ["hyperlocal","10 minute","quick commerce"],
    }
    text_lower = all_feature_text.lower()
    for feature, keywords in feature_map.items():
        if any(kw in text_lower for kw in keywords):
            result["feature_categories"].append(feature)

    for pat in [r"new\s*:\s*([^\n\r.]{5,60})", r"introducing\s+([^\n\r.]{5,60})", r"launch(?:ed|ing)?\s+([^\n\r.]{5,60})"]:
        for m in re.findall(pat, all_feature_text, re.I)[:3]:
            if len(m) > 5 and m.strip() not in result["unique_features"]:
                result["unique_features"].append(m.strip()[:80])

    tech_signals_kws = {
        "ML/AI focus":     ["machine learning","deep learning","ai","data science"],
        "React Native app":["react native","flutter","mobile engineer"],
        "Microservices":   ["microservices","kubernetes","docker","devops"],
        "Data platform":   ["data engineer","data platform","analytics"],
        "Logistics tech":  ["logistics","routing","dispatch","fleet"],
    }
    r_jobs = safe_get(base_url.rstrip("/") + "/careers")
    if r_jobs:
        jobs_text = r_jobs.text.lower()
        for signal, kws in tech_signals_kws.items():
            if any(k in jobs_text for k in kws):
                result["tech_signals"].append(signal)

    print(f"     ✅ {len(result['feature_categories'])} product areas | {len(result['unique_features'])} unique signals")
    return result
