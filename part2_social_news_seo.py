"""RivalScan v12 - Part 2: Social Media, News, SEO, GTM, Reviews, Ads"""

import re, requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from collections import Counter
from part1_utils_scraping import safe_get, extract_domain, detect_country, HEADERS


# ═══════════════════════════════════════════════════
# FIX 1 — LinkedIn: try multiple slug variants
# ═══════════════════════════════════════════════════

def scrape_facebook(facebook_url, company_name):
    print(f"  📘 Facebook...")
    fb = {"found": False, "likes": None, "followers": None, "post_frequency": "Unknown", "recent_posts": []}
    urls = []
    if facebook_url:
        urls.append(facebook_url.replace("facebook.com", "mbasic.facebook.com"))
    urls.append(f"https://mbasic.facebook.com/{company_name.lower().replace(' ','')}")

    for url in urls:
        r = safe_get(url, mobile=True)
        if not r or len(r.text) < 500:
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()
        if "isn't available" in text:
            continue
        fb["found"] = True
        for pat in [r"([\d,\.]+[KMB]?)\s*(?:people\s+)?likes?", r"([\d,\.]+[KMB]?)\s*followers?"]:
            m = re.search(pat, text, re.I)
            if m:
                if "like" in pat: fb["likes"] = m.group(1)
                else: fb["followers"] = m.group(1)
        for div in soup.find_all("div")[:20]:
            t = div.get_text(strip=True)
            if 20 < len(t) < 200:
                fb["recent_posts"].append(t[:150])
        fb["post_frequency"] = (
            "Very Active" if len(fb["recent_posts"]) >= 8 else
            "Active"      if len(fb["recent_posts"]) >= 4 else
            "Moderate"    if len(fb["recent_posts"]) >= 2 else "Low"
        )
        break

    print(f"     ✅ Facebook: {'Found - '+fb['post_frequency'] if fb['found'] else 'Not found'}")
    return fb


def scrape_linkedin(linkedin_url, company_name):
    """
    FIX 1: Try multiple LinkedIn slug variants to find correct company page.
    e.g. swiggy-in, zomato-in, companyhq, etc.
    """
    print(f"  💼 LinkedIn...")
    li = {"found": False, "followers": None, "employees": None, "industry": None, "founded": None, "specialties": [], "employees_note": ""}

    # Build slug variants — critical fix for companies like Swiggy (slug = swiggy-in)
    slug = (company_name.lower()
            .replace(" ", "-")
            .replace(".com", "")
            .replace(".ae", "")
            .replace(".in", ""))

    urls = []
    if linkedin_url:
        urls.append(linkedin_url)

    slug_variants = [
        slug,
        f"{slug}-in",          # India companies: swiggy-in, zomato-in
        f"{slug}-ae",          # UAE companies
        f"{slug}hq",           # HQ variant
        f"{slug}-official",
        slug.replace("-", ""), # no-hyphen
    ]
    for s in slug_variants:
        urls.append(f"https://www.linkedin.com/company/{s}")

    for url in urls:
        r = safe_get(url)
        if not r or len(r.text) < 500:
            continue
        text = BeautifulSoup(r.text, "html.parser").get_text(separator=" ")

        # Skip login walls or 404 pages
        if len(text) < 2000 and "join now" in text.lower():
            continue
        if "page not found" in text.lower():
            continue

        li["found"] = True

        for pat in [r"([\d,\.]+[KMB]?)\s*followers?", r"followers?\s*[:\•]\s*([\d,\.]+[KMB]?)"]:
            m = re.search(pat, text, re.I)
            if m:
                li["followers"] = m.group(1)
                break

        for pat in [r"([\d,\-]+\+?)\s*employees?", r"Company\s+size[:\s]+([\d,\-]+\+?)"]:
            m = re.search(pat, text, re.I)
            if m:
                li["employees"] = m.group(1)
                break

        m = re.search(r"Industry[:\s]+([^\n\r]{5,50})", text, re.I)
        if m:
            li["industry"] = m.group(1).strip()

        m = re.search(r"Founded[:\s]+(\d{4})", text, re.I)
        if m:
            li["founded"] = m.group(1)

        m = re.search(r"Specialties?[:\s]+([^\n\r]{10,200})", text, re.I)
        if m:
            li["specialties"] = [s.strip() for s in m.group(1).split(",")][:6]

        # Stop once we find followers — correct page found
        if li["followers"]:
            break

    # FIX 4: Flag inflated employee counts from parent company consolidation
    if li.get("employees"):
        try:
            emp_clean = re.sub(r"[^\d]", "", str(li["employees"]))
            if emp_clean and int(emp_clean) > 20000:
                li["employees_note"] = "(may include subsidiaries/parent company)"
        except:
            pass

    f = f" | {li['followers']} followers" if li["followers"] else ""
    e = f" | {li['employees']} employees" if li["employees"] else ""
    note = f" {li['employees_note']}" if li.get("employees_note") else ""
    print(f"     ✅ LinkedIn: {'Found'+f+e+note if li['found'] else 'Not found'}")
    return li


def scrape_youtube(youtube_url, company_name):
    print(f"  📺 YouTube...")
    yt = {"found": False, "subscribers": None, "recent_videos": [], "upload_frequency": "Unknown", "content_topics": []}
    urls = []
    if youtube_url:
        urls.extend([youtube_url, youtube_url.rstrip("/") + "/videos"])
    c = company_name.lower().replace(" ", "")
    urls.extend([f"https://www.youtube.com/@{c}/videos", f"https://www.youtube.com/c/{c}/videos"])

    for url in urls:
        r = safe_get(url)
        if not r or len(r.text) < 1000:
            continue
        text = r.text
        m = re.search(r'"subscriberCountText".*?"simpleText":"([^"]+)"', text)
        if m:
            yt["subscribers"] = m.group(1)
            yt["found"] = True
        titles = re.findall(r'"title":\{"runs":\[\{"text":"([^"]{10,100})"', text)
        for t in titles[:10]:
            if t not in [v["title"] for v in yt["recent_videos"]]:
                yt["recent_videos"].append({"title": t, "views": "", "published": ""})
        if yt["recent_videos"] or yt["found"]:
            yt["found"] = True
            count = len(yt["recent_videos"])
            yt["upload_frequency"] = (
                "Very Active" if count >= 8 else "Active" if count >= 4 else
                "Moderate"    if count >= 2 else "Inactive"
            )
            yt["content_topics"] = list(set(re.findall(r"\b[A-Z][a-z]{3,}(?:\s[A-Z][a-z]{3,})*\b",
                " ".join([v["title"] for v in yt["recent_videos"]]))))[:8]
            break

    s = f" | {yt['subscribers']} subs" if yt["subscribers"] else ""
    print(f"     ✅ YouTube: {'Found - '+yt['upload_frequency']+s if yt['found'] else 'Not found'}")
    return yt


def scrape_twitter(twitter_url, company_name):
    print(f"  🐦 Twitter/X...")
    tw = {"found": False, "followers": None, "bio": None, "recent_tweets": [], "posting_freq": "Unknown"}
    urls = []
    if twitter_url:
        handle = twitter_url.rstrip("/").split("/")[-1]
        urls.extend([f"https://nitter.net/{handle}", f"https://nitter.privacydev.net/{handle}"])
    c = company_name.lower().replace(" ", "")
    urls.extend([f"https://nitter.net/{c}", f"https://nitter.privacydev.net/{c}"])

    for url in urls:
        r = safe_get(url)
        if not r or len(r.text) < 500:
            continue
        soup    = BeautifulSoup(r.text, "html.parser")
        profile = soup.find("div", class_="profile-card")
        if profile:
            tw["found"] = True
            bio_el = profile.find("p", class_="profile-bio")
            if bio_el:
                tw["bio"] = bio_el.get_text(strip=True)[:200]
            stats = profile.find_all("span", class_="profile-stat-num")
            if len(stats) >= 3:
                tw["followers"] = stats[2].get_text(strip=True)
            for t in soup.find_all("div", class_="tweet-content")[:10]:
                text = t.get_text(strip=True)
                if len(text) > 10:
                    tw["recent_tweets"].append(text[:200])
            tw["posting_freq"] = (
                "Very Active" if len(tw["recent_tweets"]) >= 8 else
                "Active"      if len(tw["recent_tweets"]) >= 4 else
                "Moderate"    if len(tw["recent_tweets"]) >= 2 else "Low"
            )
            break

    f = f" | {tw['followers']} followers" if tw["followers"] else ""
    print(f"     ✅ Twitter/X: {'Found'+f if tw['found'] else 'Limited (blocked)'}")
    return tw


# ═══════════════════════════════════════════════════
# MODULE 11: NEWS
# ═══════════════════════════════════════════════════

def scrape_news(company_name, domain):
    print(f"  📰 News...")
    news = {
        "recent_articles": [], "total_found": 0, "media_presence": "Unknown",
        "latest_headline": None, "funding_mentioned": False,
        "acquisition_mentioned": False, "partnership_mentioned": False,
        "award_mentioned": False, "negative_mentioned": False, "signals_summary": [],
    }
    country      = detect_country(domain)
    country_name = {"ae":"UAE","uk":"UK","au":"Australia","in":"India","sg":"Singapore","sa":"Saudi Arabia","qa":"Qatar"}.get(country, "")
    queries      = [f'"{domain}"']
    if country_name:
        queries.append(f'"{company_name}" {country_name}')
    else:
        queries.append(f'"{company_name}"')

    for query in queries:
        try:
            url  = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
            r    = safe_get(url)
            if not r:
                continue
            soup = BeautifulSoup(r.content, "xml")
            for item in soup.find_all("item")[:8]:
                title   = item.find("title")
                source  = item.find("source")
                pubdate = item.find("pubDate")
                if not title:
                    continue
                article = {
                    "title":  title.get_text(strip=True)[:150],
                    "source": source.get_text(strip=True) if source else "News",
                    "date":   pubdate.get_text(strip=True)[:20] if pubdate else "",
                }
                if article["title"] not in [a["title"] for a in news["recent_articles"]]:
                    news["recent_articles"].append(article)
                tl = article["title"].lower()
                if any(k in tl for k in ["raises","funding","series","million","billion"]): news["funding_mentioned"]     = True
                if any(k in tl for k in ["acquires","acquired","merger","acquisition"]):    news["acquisition_mentioned"] = True
                if any(k in tl for k in ["partner","partnership","collaboration"]):         news["partnership_mentioned"] = True
                if any(k in tl for k in ["award","wins","winner","best","top","ranked"]):   news["award_mentioned"]       = True
                if any(k in tl for k in ["layoff","lawsuit","fraud","scandal","fine"]):     news["negative_mentioned"]    = True
        except:
            pass

    news["total_found"]  = len(news["recent_articles"])
    news["latest_headline"] = news["recent_articles"][0]["title"] if news["recent_articles"] else None
    count = news["total_found"]
    news["media_presence"] = (
        "Very High" if count > 15 else "High"   if count > 8 else
        "Medium"    if count > 3  else "Low"     if count > 0 else "None detected"
    )
    if news["funding_mentioned"]:     news["signals_summary"].append("Funding Activity")
    if news["acquisition_mentioned"]: news["signals_summary"].append("M&A Activity")
    if news["partnership_mentioned"]: news["signals_summary"].append("Partnership Announced")
    if news["award_mentioned"]:       news["signals_summary"].append("Award/Recognition")
    if news["negative_mentioned"]:    news["signals_summary"].append("Negative Press Detected")
    print(f"     ✅ {count} articles | {news['media_presence']} | {', '.join(news['signals_summary']) or 'No signals'}")
    return news


# ═══════════════════════════════════════════════════
# SEO MODULE 1: SITEMAP
# ═══════════════════════════════════════════════════

def analyze_sitemap(base_url):
    print(f"  🗺️  Sitemap...")
    data = {
        "found": False, "sitemap_url": None, "total_urls": 0,
        "page_types": {}, "top_sections": [], "has_news_sitemap": False,
        "seo_footprint": "Not found",
    }
    from urllib.parse import urlparse
    robots = safe_get(base_url.rstrip("/") + "/robots.txt")
    sitemap_paths = ["/sitemap.xml", "/sitemap_index.xml", "/sitemap1.xml", "/wp-sitemap.xml"]
    if robots:
        m = re.search(r"Sitemap:\s*(.+)", robots.text, re.I)
        if m:
            su = m.group(1).strip()
            sitemap_paths.insert(0, su if su.startswith("http") else "/" + su)

    all_urls = []
    for path in sitemap_paths:
        url = path if path.startswith("http") else base_url.rstrip("/") + path
        r   = safe_get(url)
        if not r or len(r.text) < 100:
            continue
        data["found"] = True
        data["sitemap_url"] = url
        soup = BeautifulSoup(r.text, "xml")
        for loc_tag in soup.find_all("loc"):
            all_urls.append(loc_tag.get_text(strip=True))
        if "news" in url.lower():
            data["has_news_sitemap"] = True
        break

    data["total_urls"] = len(all_urls)
    section_counter = Counter()
    for url in all_urls[:500]:
        path  = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        if parts:
            section_counter[parts[0]] += 1
    data["top_sections"] = [f"/{s} ({c} pages)" for s, c in section_counter.most_common(8)]
    data["seo_footprint"] = (
        "Massive (1000+ pages)" if data["total_urls"] >= 1000 else
        "Large (500+ pages)"    if data["total_urls"] >= 500  else
        "Medium (100+ pages)"   if data["total_urls"] >= 100  else
        "Small (50+ pages)"     if data["total_urls"] >= 50   else
        "Minimal (<50 pages)"   if data["total_urls"] > 0     else
        "Not found"
    )
    print(f"     ✅ {data['total_urls']} pages | {data['seo_footprint']}")
    return data


# ═══════════════════════════════════════════════════
# SEO MODULE 2: ROBOTS.TXT
# ═══════════════════════════════════════════════════

def analyze_robots(base_url):
    print(f"  🤖 Robots.txt...")
    data = {"found": False, "blocked_paths": [], "allowed_paths": [], "sitemap_declared": False, "intelligence": []}
    r = safe_get(base_url.rstrip("/") + "/robots.txt")
    if not r or len(r.text) < 10:
        print(f"     ℹ Not found")
        return data
    data["found"] = True
    for line in r.text.split("\n"):
        line = line.strip()
        if line.lower().startswith("disallow:"):
            path = line.split(":", 1)[1].strip()
            if path and path != "/":
                data["blocked_paths"].append(path)
        elif line.lower().startswith("sitemap:"):
            data["sitemap_declared"] = True

    blocked = " ".join(data["blocked_paths"]).lower()
    if "/admin"    in blocked: data["intelligence"].append("Has admin panel — likely WordPress/CMS")
    if "/checkout" in blocked or "/cart" in blocked: data["intelligence"].append("Has e-commerce checkout flow")
    if "/api"      in blocked: data["intelligence"].append("Has internal API — tech-heavy product")
    if "/user"     in blocked or "/account" in blocked: data["intelligence"].append("Has user accounts/login system")
    if "/wp-"      in blocked: data["intelligence"].append("Confirmed WordPress site")
    if "/staging"  in blocked or "/dev" in blocked: data["intelligence"].append("Has staging/development environment")
    print(f"     ✅ {len(data['blocked_paths'])} blocked paths | {len(data['intelligence'])} signals")
    return data


# ═══════════════════════════════════════════════════
# SEO MODULE 3: ON-PAGE AUDIT
# ═══════════════════════════════════════════════════

def analyze_onpage_seo(url):
    print(f"  📊 On-Page SEO Audit...")
    data = {
        "score": 0, "max_score": 100, "grade": "F",
        "checks": {}, "quick_wins": [], "strengths": [],
        "keyword_focus": [], "heading_structure": {}, "meta_analysis": {},
        "schema_types": [], "internal_links": 0, "external_links": 0,
        "images_total": 0, "images_with_alt": 0, "images_without_alt": 0,
        "word_count": 0, "page_size_kb": 0,
        "has_canonical": False, "has_og_tags": False,
        "has_twitter_cards": False, "has_ssl": False,
    }
    import json as _json
    r = safe_get(url)
    if not r:
        return data

    data["page_size_kb"] = round(len(r.content) / 1024, 1)
    data["has_ssl"]      = url.startswith("https")
    soup  = BeautifulSoup(r.text, "html.parser")
    score = 0

    title = soup.find("title")
    if title:
        title_text = title.get_text(strip=True)
        title_len  = len(title_text)
        data["meta_analysis"]["title"]     = title_text
        data["meta_analysis"]["title_len"] = title_len
        if 30 <= title_len <= 60:
            score += 10; data["strengths"].append(f"[STRONG] Title tag perfect length ({title_len} chars)"); data["checks"]["Title Tag"] = "[OK] Good"
        elif title_len > 0:
            score += 5;  data["quick_wins"].append(f"Title tag is {title_len} chars — optimal is 30-60"); data["checks"]["Title Tag"] = "[!] Needs work"
        else:
            data["quick_wins"].append("Missing title tag — critical SEO issue"); data["checks"]["Title Tag"] = "[X] Missing"
    else:
        data["quick_wins"].append("No title tag found — critical SEO issue"); data["checks"]["Title Tag"] = "[X] Missing"

    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        desc_text = meta_desc.get("content", "")
        desc_len  = len(desc_text)
        data["meta_analysis"]["description"]     = desc_text
        data["meta_analysis"]["description_len"] = desc_len
        if 120 <= desc_len <= 160:
            score += 8; data["strengths"].append(f"[STRONG] Meta description perfect ({desc_len} chars)"); data["checks"]["Meta Description"] = "[OK] Good"
        elif desc_len > 0:
            score += 4; data["quick_wins"].append(f"Meta description is {desc_len} chars — optimal is 120-160"); data["checks"]["Meta Description"] = "[!] Needs work"
        else:
            data["quick_wins"].append("Meta description empty — write one with your main keyword"); data["checks"]["Meta Description"] = "[X] Empty"
    else:
        data["quick_wins"].append("No meta description — add one immediately"); data["checks"]["Meta Description"] = "[X] Missing"

    for tag in ["h1","h2","h3","h4"]:
        data["heading_structure"][tag] = [h.get_text(strip=True)[:80] for h in soup.find_all(tag)[:5]]

    h1_tags = soup.find_all("h1")
    if len(h1_tags) == 1:
        score += 10; data["strengths"].append("[STRONG] Single H1 tag (perfect)"); data["checks"]["H1 Tag"] = "[OK] Good"
    elif len(h1_tags) == 0:
        data["quick_wins"].append("No H1 tag — add one with your main keyword"); data["checks"]["H1 Tag"] = "[X] Missing"
    else:
        score += 3;  data["quick_wins"].append(f"Multiple H1 tags ({len(h1_tags)}) — should have exactly one"); data["checks"]["H1 Tag"] = f"[!] {len(h1_tags)} H1s"

    h2_count = len(soup.find_all("h2"))
    if h2_count >= 2:
        score += 5; data["strengths"].append(f"[STRONG] {h2_count} H2 tags for structure"); data["checks"]["H2 Tags"] = f"[OK] {h2_count} found"
    else:
        data["quick_wins"].append("Add H2 subheadings to structure your content"); data["checks"]["H2 Tags"] = "[!] Few or none"

    images = soup.find_all("img")
    data["images_total"]       = len(images)
    data["images_with_alt"]    = sum(1 for img in images if img.get("alt","").strip())
    data["images_without_alt"] = data["images_total"] - data["images_with_alt"]
    if data["images_total"] == 0:
        data["checks"]["Image ALT Tags"] = "[i] No images"
    elif data["images_without_alt"] == 0:
        score += 8; data["strengths"].append(f"[STRONG] All {data['images_total']} images have ALT text"); data["checks"]["Image ALT Tags"] = "[OK] All good"
    else:
        score += 3; data["quick_wins"].append(f"{data['images_without_alt']} images missing ALT text"); data["checks"]["Image ALT Tags"] = f"[!] {data['images_without_alt']} missing"

    canonical = soup.find("link", attrs={"rel": "canonical"})
    if canonical:
        data["has_canonical"] = True; score += 5; data["strengths"].append("[STRONG] Canonical tag present"); data["checks"]["Canonical Tag"] = "[OK] Present"
    else:
        data["quick_wins"].append("Add canonical tag to prevent duplicate content issues"); data["checks"]["Canonical Tag"] = "[X] Missing"

    og_title = soup.find("meta", property="og:title")
    if og_title:
        data["has_og_tags"] = True; score += 5; data["strengths"].append("[STRONG] Open Graph tags for social sharing"); data["checks"]["Open Graph"] = "[OK] Present"
    else:
        data["quick_wins"].append("Add Open Graph tags — improves how links look on LinkedIn/Facebook"); data["checks"]["Open Graph"] = "[X] Missing"

    tw_card = soup.find("meta", attrs={"name": "twitter:card"})
    if tw_card:
        data["has_twitter_cards"] = True; score += 3; data["checks"]["Twitter Cards"] = "[OK] Present"
    else:
        data["checks"]["Twitter Cards"] = "[X] Missing"

    schema_types = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            sd = _json.loads(script.string or "{}")
            st = sd.get("@type", "")
            if st:
                schema_types.append(st)
        except:
            pass
    data["schema_types"] = schema_types
    if schema_types:
        score += 8; data["strengths"].append(f"[STRONG] Schema markup: {', '.join(schema_types[:3])}"); data["checks"]["Schema Markup"] = f"[OK] {', '.join(schema_types[:3])}"
    else:
        data["quick_wins"].append("Add Schema markup (JSON-LD) — helps Google understand your content"); data["checks"]["Schema Markup"] = "[X] Missing"

    if data["has_ssl"]:
        score += 5; data["strengths"].append("[STRONG] HTTPS/SSL enabled"); data["checks"]["HTTPS/SSL"] = "[OK] Secure"
    else:
        data["quick_wins"].append("Enable HTTPS — Google penalizes non-HTTPS sites"); data["checks"]["HTTPS/SSL"] = "[X] Not secure"

    from part1_utils_scraping import extract_domain as _ed
    base_domain = _ed(url)
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.startswith("http") and base_domain not in href: data["external_links"] += 1
        elif href.startswith("/") or base_domain in href:        data["internal_links"] += 1

    if data["internal_links"] >= 5:
        score += 5; data["strengths"].append(f"[STRONG] {data['internal_links']} internal links"); data["checks"]["Internal Links"] = f"[OK] {data['internal_links']}"
    else:
        data["quick_wins"].append(f"Only {data['internal_links']} internal links — add more"); data["checks"]["Internal Links"] = f"[!] Only {data['internal_links']}"

    for tag in soup(["script","style","nav","footer"]):
        tag.decompose()
    body_text        = soup.get_text(separator=" ", strip=True)
    words            = body_text.split()
    data["word_count"] = len(words)
    if data["word_count"] >= 500:
        score += 8; data["strengths"].append(f"[STRONG] Good content length ({data['word_count']} words)"); data["checks"]["Content Length"] = f"[OK] {data['word_count']} words"
    elif data["word_count"] >= 200:
        score += 4; data["checks"]["Content Length"] = f"[!] {data['word_count']} words (aim for 500+)"
    else:
        data["quick_wins"].append(f"Homepage only has {data['word_count']} words — too thin"); data["checks"]["Content Length"] = f"[X] {data['word_count']} words"

    if data["page_size_kb"] < 500:
        score += 5; data["strengths"].append(f"[STRONG] Page size good ({data['page_size_kb']}KB)"); data["checks"]["Page Size"] = f"[OK] {data['page_size_kb']}KB"
    else:
        data["checks"]["Page Size"] = f"[!] {data['page_size_kb']}KB"

    stop_words = {"the","a","an","and","or","but","in","on","at","to","for","of","with","by","from","is","are","was","be","this","that","we","you","it","not","so","if","as","our","your","their","how","what","when","who","which","more","also","just","like","get","all","one","any","very","most","have","has","do","does","will","can","may","new","use","used","best"}
    word_freq = Counter(w.lower() for w in words if len(w) > 3 and w.lower() not in stop_words and w.isalpha())
    data["keyword_focus"] = [word for word, count in word_freq.most_common(10)]

    data["score"] = min(score, 100)
    data["grade"] = "A" if score >= 80 else "B" if score >= 65 else "C" if score >= 50 else "D" if score >= 35 else "F"
    print(f"     ✅ SEO Score: {data['score']}/100 (Grade: {data['grade']}) | {len(data['quick_wins'])} issues")
    return data


# ═══════════════════════════════════════════════════
# SEO MODULE 4: KEYWORD POSITIONING + LANDING PAGES
# ═══════════════════════════════════════════════════

def analyze_keyword_positioning(url, company_name, domain):
    print(f"  🔍 Indexed Pages + Landing Pages...")
    data = {
        "google_indexed_pages": None, "google_indexed_raw": None,
        "landing_pages_found": [], "landing_page_paths": [],
        "subdomains_found": [], "ad_landing_pages": [],
        "top_keywords_from_content": [], "positioning_signals": [],
    }

    # Real Google index count
    try:
        r = requests.get(
            f"https://www.google.com/search?q=site:{domain}&hl=en&num=10",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36", "Accept-Language": "en-US,en;q=0.9"},
            timeout=12
        )
        if r and r.status_code == 200:
            for pat in [r"About\s+([\d,]+)\s+results?", r"([\d,]+)\s+results?"]:
                m = re.search(pat, r.text, re.I)
                if m:
                    raw = m.group(1).replace(",", "")
                    if raw.isdigit() and int(raw) > 0:
                        data["google_indexed_pages"] = m.group(1)
                        data["google_indexed_raw"]   = int(raw)
                        break
    except:
        pass

    # Landing page detection (40+ paths)
    LP_PATHS = [
        "/lp","/landing","/offer","/offers","/campaign","/campaigns",
        "/promo","/promotions","/deal","/deals","/special","/sale",
        "/ads/","/ppc","/sem","/buy","/get-started","/start",
        "/quote","/get-quote","/compare","/comparison",
        "/features","/benefits","/why-us","/why-choose",
        "/app","/download","/mobile","/free","/trial","/demo",
        "/signup","/register","/dubai","/uae","/partners",
    ]
    protocol = "https://" if url.startswith("https") else "http://"
    for path in LP_PATHS:
        test_url = f"{protocol}{domain}{path}"
        try:
            r_lp = requests.get(test_url, headers=HEADERS, timeout=6, allow_redirects=True)
            if not r_lp or r_lp.status_code not in [200, 301, 302]:
                continue
            if len(r_lp.text) < 500:
                continue
            soup_lp   = BeautifulSoup(r_lp.text, "html.parser")
            page_text = soup_lp.get_text(strip=True).lower()
            is_404    = any(k in page_text[:200] for k in ["404","not found","page doesn't exist","page not found","oops"])
            if is_404:
                continue
            is_ad = any(k in page_text for k in ["utm_source","gclid","fbclid","noindex","get a quote","limited offer","act now","claim your"])
            data["landing_pages_found"].append(path)
            data["landing_page_paths"].append({"path": path, "url": r_lp.url[:100], "is_ad": is_ad, "size_kb": round(len(r_lp.content)/1024, 1)})
            if is_ad:
                data["ad_landing_pages"].append(path)
        except:
            continue

    # Subdomain enumeration
    COMMON_SUBDOMAINS = ["blog","news","shop","store","app","api","m","mobile","go","lp","landing","offers","deals","promo","campaign","ads","help","support","docs","careers","jobs","pay","payments","checkout","billing"]
    base_parts   = domain.split(".")
    base_domain  = ".".join(base_parts[-2:]) if len(base_parts) > 2 else domain
    for sub in COMMON_SUBDOMAINS:
        test_url_sub = f"https://{sub}.{base_domain}"
        try:
            r_sub = requests.get(test_url_sub, headers=HEADERS, timeout=5, allow_redirects=True)
            if r_sub and r_sub.status_code == 200 and len(r_sub.text) > 200 and base_domain in r_sub.url:
                data["subdomains_found"].append({"subdomain": f"{sub}.{base_domain}", "url": r_sub.url[:80], "type": sub, "size_kb": round(len(r_sub.content)/1024, 1)})
        except:
            continue

    # Keyword analysis from homepage
    r = safe_get(url)
    if r:
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script","style","nav","footer"]):
            tag.decompose()
        text  = soup.get_text(separator=" ", strip=True).lower()
        words = text.split()
        stop  = {"the","a","an","and","or","but","in","on","at","to","for","of","with","by","from","is","are","was","be","this","that","we","you","it","not","so","if","as","our","your","their","how","what","when","who","which","more","also","just","like","get","all","one","any","very","most","have","has","do","does","will","can","may","new","use","used","best","help"}
        freq  = Counter(w for w in words if len(w) > 3 and w not in stop and w.isalpha())
        data["top_keywords_from_content"] = [w for w, _ in freq.most_common(15)]
        cs = " ".join(data["top_keywords_from_content"])
        if any(k in cs for k in ["insurance","policy","cover","claim","premium"]):   data["positioning_signals"].append("Insurance/financial services")
        if any(k in cs for k in ["food","restaurant","delivery","order","menu"]):    data["positioning_signals"].append("Food delivery/restaurant")
        if any(k in cs for k in ["saas","software","platform","cloud","api"]):       data["positioning_signals"].append("SaaS/technology")
        if any(k in cs for k in ["compare","comparison","quote","cheapest"]):         data["positioning_signals"].append("Comparison/aggregator")
        if any(k in cs for k in ["free","trial","demo","signup"]):                    data["positioning_signals"].append("Freemium/trial model")

    indexed   = data["google_indexed_pages"] or "Unknown"
    lp_count  = len(data["landing_pages_found"])
    sub_count = len(data["subdomains_found"])
    print(f"     ✅ Google indexed: {indexed} | Landing pages: {lp_count} | Subdomains: {sub_count}")
    return data


def full_seo_scan(url, company_name=None):
    if not company_name:
        company_name = extract_domain(url).split(".")[0].capitalize()
    domain = extract_domain(url)
    print(f"\n  🔍 SEO SCAN: {company_name}")
    sitemap  = analyze_sitemap(url)
    robots   = analyze_robots(url)
    onpage   = analyze_onpage_seo(url)
    keywords = analyze_keyword_positioning(url, company_name, domain)
    return {"company_name": company_name, "url": url, "domain": domain, "sitemap": sitemap, "robots": robots, "onpage": onpage, "keywords": keywords}


# ═══════════════════════════════════════════════════
# GTM DEEP SCAN
# ═══════════════════════════════════════════════════

def scan_gtm_deep(url):
    print(f"  🔬 GTM Deep Scan...")
    data = {"hidden_tools": [], "gtm_ids": [], "data_layer_events": [], "extra_tools": []}
    r = safe_get(url)
    if not r:
        return data
    html       = r.text
    html_lower = html.lower()
    data["gtm_ids"] = list(set(re.findall(r"GTM-[A-Z0-9]+", html)))

    hidden_detections = {
        "Google Analytics 4":     ["gtag('config'","g-","ga4","measurement_id"],
        "Google Ads Remarketing":  ["aw-","google_conversion","googleadservices"],
        "Facebook Pixel":          ["fbq('init'","pixel_id","facebook.com/tr"],
        "HotJar":                  ["hjid","hotjar","hjsv"],
        "Intercom":                ["intercomsettings","app_id","intercom("],
        "Zendesk":                 ["zesettings","ze('webwidget'","zdplugin"],
        "Crisp Chat":              ["crisp.chat","$crisp","crisp_website_id"],
        "Tawk.to":                 ["tawk.to","s1.src","tawkto"],
        "LiveChat":                ["livechat","__lc","lc_api"],
        "Drift":                   ["drift.load","driftt","dt=new"],
        "Mixpanel":                ["mixpanel.init","mp_","mixpanel.track"],
        "Segment":                 ["analytics.load","analytics.identify","cdn.segment"],
        "Heap":                    ["heap.load","heap.appid","window.heap"],
        "Klaviyo":                 ["klaviyo.init","_learnq","klaviyo.com"],
        "HubSpot Forms":           ["hbspt.forms","hs-form","hsforms.net"],
        "Stripe":                  ["stripe.js","pk_live","pk_test","stripe('"],
        "WhatsApp Widget":         ["whatsapp","wa.me","whatsapp.com/send"],
        "LinkedIn Insight":        ["_linkedin_partner","licdn","linkedin insight"],
        "TikTok Pixel":            ["ttq.load","analytics.tiktok"],
        "Salesforce":              ["salesforce","pardot","sfdc"],
        "Freshchat":               ["freshchat","fcwidget","freshworks"],
        "Cookie Consent":          ["cookieyes","cookiebot","onetrust","gdpr"],
        "Push Notifications":      ["onesignal","pushcrew","webpush"],
        "Reviews Widget":          ["trustpilot","reviews.io","yotpo"],
        "Booking System":          ["calendly","acuity","bookings","schedule"],
    }
    for tool, signals in hidden_detections.items():
        if any(s in html_lower for s in signals):
            if tool not in data["hidden_tools"]:
                data["hidden_tools"].append(tool)

    soup = BeautifulSoup(html, "html.parser")
    gen  = soup.find("meta", attrs={"name": "generator"})
    if gen:
        data["extra_tools"].append(f"CMS: {gen.get('content','')[:50]}")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "apps.apple.com" in href:  data["extra_tools"].append("iOS App available")
        elif "play.google.com" in href: data["extra_tools"].append("Android App available")
    wa_links = [a["href"] for a in soup.find_all("a", href=True) if "wa.me" in a["href"] or "whatsapp" in a["href"].lower()]
    if wa_links:
        data["extra_tools"].append(f"WhatsApp Business: {wa_links[0][:60]}")

    print(f"     ✅ {len(data['hidden_tools'])} hidden tools | GTM IDs: {', '.join(data['gtm_ids']) or 'None'}")
    return data


# ═══════════════════════════════════════════════════
# REVIEWS
# ═══════════════════════════════════════════════════

def scrape_reviews(company_name, domain, base_url):
    print(f"  ⭐ Reviews...")
    reviews = {"google_rating": None, "google_count": None, "trustpilot_rating": None, "trustpilot_count": None, "site_rating": None, "site_count": None, "review_signals": []}
    r = safe_get(base_url)
    if r:
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text()
        for pat in [r"(\d+\.\d+)\s*(?:out of\s*)?(?:\/\s*)?5\s*(?:stars?)?", r"rated\s+(\d+\.\d+)", r"(\d+\.\d+)\s*stars?"]:
            m = re.search(pat, text, re.I)
            if m:
                rating = float(m.group(1))
                if 1.0 <= rating <= 5.0:
                    reviews["site_rating"] = rating
                    break
        for pat in [r"([\d,]+)\+?\s*(?:customer\s+)?reviews?", r"([\d,]+)\+?\s*(?:verified\s+)?ratings?"]:
            m = re.search(pat, text, re.I)
            if m:
                reviews["site_count"] = m.group(1).replace(",", "")
                break
        html_lower = r.text.lower()
        if "trustpilot" in html_lower:    reviews["review_signals"].append("Trustpilot widget present")
        if "google" in html_lower and "review" in html_lower: reviews["review_signals"].append("Google reviews referenced")
    try:
        tp_url = f"https://www.trustpilot.com/review/{domain}"
        r2     = safe_get(tp_url)
        if r2 and "trustpilot" in r2.url.lower():
            soup2 = BeautifulSoup(r2.text, "html.parser")
            text2 = soup2.get_text()
            m = re.search(r"TrustScore\s+([\d.]+)", text2, re.I)
            if m: reviews["trustpilot_rating"] = m.group(1)
            m = re.search(r"([\d,]+)\s+reviews?", text2, re.I)
            if m: reviews["trustpilot_count"] = m.group(1)
    except:
        pass
    rating_str = f"Site: {reviews['site_rating']}/5" if reviews["site_rating"] else "Not found on site"
    print(f"     ✅ Rating: {rating_str} | Count: {reviews['site_count'] or 'Not detected'}")
    return reviews


# ═══════════════════════════════════════════════════
# AD INTELLIGENCE
# ═══════════════════════════════════════════════════

def scan_ad_intelligence(domain, company_name):
    print(f"  📢 Ad Intelligence...")
    ads = {"facebook_ads_active": False, "google_ads_detected": False, "ad_messaging": [], "ad_ctas": [], "estimated_ad_spend": "Unknown"}
    r2 = safe_get(f"https://{domain}")
    if r2:
        html = r2.text.lower()
        if any(s in html for s in ["googleadservices","google_conversion","aw-","adwords"]):
            ads["google_ads_detected"] = True
        soup = BeautifulSoup(r2.text, "html.parser")
        for btn in soup.find_all(["button","a"], class_=re.compile(r"btn|cta|hero|primary", re.I))[:10]:
            t = btn.get_text(strip=True)
            if t and 3 < len(t) < 50:
                ads["ad_ctas"].append(t)
    ads["estimated_ad_spend"] = (
        "Medium (Google Ads active)" if ads["google_ads_detected"] else "Low or organic-only"
    )
    print(f"     ✅ Google Ads: {'Yes' if ads['google_ads_detected'] else 'No'} | Spend: {ads['estimated_ad_spend']}")
    return ads
