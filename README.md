# RivalScan.ai 🔍
### AI-Powered Competitive Intelligence Engine

Enter any two URLs → get a full 18-page PDF intelligence report in minutes.

> "Crayon costs $15,000/year. RivalScan costs $199 per report."

---

## What It Does

RivalScan automatically collects and analyzes publicly available data about any two companies and generates a professional PDF report covering:

- ✅ Executive Intelligence Dashboard (28 metrics side by side)
- ✅ Tech Stack Detection (40+ tools)
- ✅ Hidden Tech Stack via GTM Deep Scan
- ✅ SEO Intelligence (score, sitemap, indexed pages, landing pages)
- ✅ Social Media (LinkedIn, YouTube, Facebook, Twitter/X)
- ✅ News & PR Intelligence
- ✅ Hiring Intelligence (open roles by department)
- ✅ App Store Ratings & Sentiment
- ✅ Reddit Sentiment Analysis
- ✅ Facebook & Google Ad Intelligence
- ✅ Pricing Intelligence
- ✅ Geographic Footprint
- ✅ AI-Generated 30-Day Battle Plan

---

## Sample Report

**Swiggy vs Zomato** — Generated April 2026

Key findings:
- Zomato: 1.55M LinkedIn followers, SEO Grade A, 58% market share
- Swiggy: JS-rendered site, limited data, aggressive hiring signal
- AI identified 5 urgent threats and a 30-day battle plan

---

## Setup

**1. Install dependencies:**
pip install anthropic requests beautifulsoup4 python-dotenv reportlab lxml

**2. Create a `.env` file in the project folder:**
ANTHROPIC_API_KEY=sk-ant-...
HUNTER_API_KEY=...
GOOGLE_API_KEY=AIzaSy...
NEWS_API_KEY=...

**3. Run:**
python rivalscan_master.py

**4. Enter two URLs when prompted:**
Enter YOUR website URL: https://insurancemarket.ae
Enter COMPETITOR URL:   https://bayzat.com

**5. Report generates in 8-10 minutes as a PDF.**

---

## API Keys Required

| API | Cost | Get It |
|-----|------|--------|
| Anthropic (Claude) | Paid | platform.anthropic.com |
| Hunter.io | Free 25/mo | hunter.io |
| Google PageSpeed | Free | console.cloud.google.com |
| NewsAPI | Free 100/day | newsapi.org |

---

## File Structure
rivalscan/
├── part1_utils_scraping.py      # Core scraping modules 1-6
├── part2_social_news_seo.py     # Social, news, SEO, GTM, ads
├── part3_apis_intelligence.py   # API modules + intelligence
├── rivalscan_master.py          # Main file — run this
└── .env                         # Your API keys (not on GitHub)

---

## Built With

- Python 3.x
- Claude API (Anthropic) — AI analysis
- ReportLab — PDF generation
- BeautifulSoup4 — Web scraping
- Hunter.io, Google PageSpeed, NewsAPI, YouTube Data API

---

*RivalScan.ai — Built by Anoop Shekhar*
