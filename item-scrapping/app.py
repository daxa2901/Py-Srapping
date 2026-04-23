import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
from markdownify import markdownify as md
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

# -------------------------------
# Fetch Mode Configuration
# "requests" : simple HTTP fetch
# "js"        : headless browser via Playwright (for JS-rendered sites)
# "zyte"      : Zyte API (for heavily protected / bot-blocked sites)
# -------------------------------
ZYTE_API_KEY = "768bc72deb33498aa2afe38374248461"  # replace with your key

DOMAIN_FETCH_MODE = {
    "bisonoffice.com": "requests",
    "cymax.com": "zyte",
    "homesquare.com": "zyte",
    "diningroomsoutlet.com" :"requests",
    "bedroomfurniturediscounts.com" : "zyte",
    "emmamason.com" : "zyte",
    "homegallerystores.com": "zyte",
    "colemanfurniture.com" : "zyte",
    "walmart.com": "zyte"
}

# -------------------------------
# Domain Selector Configuration
# -------------------------------
DOMAIN_SELECTORS = {
    "bisonoffice.com": {
        "title": ["#app > div.product-main__info > div.product-main__info-name"],
        "price": ["#app > div.product-main__info > div.product-main__info-prices"],
        "description" : ["#app > div.product-main__info > div.products-main__filter > div:nth-child(1) > div.products-main__filter-item-content"],
        "specifications" : ["#app > div.product-main__info > div.products-main__filter > div:nth-child(2) > div.products-main__filter-item-content > ul"],
        "image" : ["#img-container"]
    },
    "cymax.com" : {
        "title": ["#product-title-review"], 
        "price": ["#product-price"], 
        "specifications" : ["#product-details","#product-dimensions > p"], 
        "image" : ["#product-image-gallery"]
    },
    "homesquare.com": {
        "title": ["#linkToTitle"], 
        "price": ["div.allMainPrices"], 
        "specifications" : ["#product-details","#product-dimensions > p"], 
        "image" : ["#galleryImagesNav > div > div"]
    },
    "diningroomsoutlet.com": {
        "title": ["div.Product.product-view > div.Product__main.col-2 > div.Product__main-details > div.Product__name"], 
        "price": ["div.Product__checkout-add-to-cart-price"], 
        "specifications" : [
            "div.product-view > div.Product.product-view > div.Product__main.col-2 > div.Product__main-details > div.std",
            "#product-details-wrapper > div.Product__additional"

            ], 
        "description" : ["#product-details-wrapper > div.Product__description"],
        "image" : ["div.Product__media >div.more-views"]
    },
    "bedroomfurniturediscounts.com" : {
        "title": ["div.Product.product-view > div.Product__main.col-2 > div.Product__main-details > div.Product__name"], 
        "price": ["div.Product__checkout-add-to-cart-price"], 
        "specifications" : [
            "div.product-view > div.Product.product-view > div.Product__main.col-2 > div.Product__main-details > div.std",
            "#product-details-wrapper > div.Product__additional"

            ], 
        "description" : ["#product-details-wrapper > div.Product__description"],
        "image" : ["div.Product__media >div.more-views"]
    },
    "emmamason.com" : {
        "title" : ["div.Product__info-main h1"],
        "price" : ["div.product-info-price >div.price-box"],
        "specifications" : [
            "div.Product__info-main.product-info-main-wrapper.product-info-main > div.product.attribute.overview",
            "#product-attribute-specs-table"
        ],
        "image" : ["div.Product__media > div.gallery-thumbnails-desktop.js-thumbnails-desktop"]
    },
    "homegallerystores.com" : {
        "title" : ["#ssr-shell-catalog-detail-containers-DetailPage .pdp-info .pdp-title"],
        "price" : ["#ssr-shell-catalog-detail-containers-DetailPage .pdp-info .pdp-price-content"],
        "specifications" : ["#ssr-shell-catalog-detail-containers-DetailPage .pdp-info .tab-content"],
        "image" : [".image-gallery-thumbnails-container"]
    },
    "colemanfurniture.com" : {
        "title" : ["div.pdp-info h1.pdp-title"],
        "price" : ["div.price-affirm-main"],
        "specifications" : [
            "div.pdp-info ul.accordion",
        ],
        "image" : ["div.image-gallery-thumbnails"]
    },
    "walmart.com" : {
        "title" : ["#main-title"],
        "price" : ["#maincontent > section > main > div.flex.flex-column.h-100 > div:nth-child(2) > div > div.GridColumn_gridColumn__a_mwP.GridColumn_gutter__wx4JI.GridColumn_small3__OOahY > div > div:nth-child(1) > div > div:nth-child(2) > div > div > div.flex.flex-wrap.items-center > section"],
        "description" : ["#item-product-details > div.Collapse_collapse__Ja6XL.expand-collapse-content > div > div > div:nth-child(1) > span > div > p"],
        "specifications" : [
            "#item-product-details > div.Collapse_collapse__Ja6XL.expand-collapse-content > div",
            "#maincontent > section > main > div.flex.flex-column.h-100 > div:nth-child(3) > div > div.GridColumn_gridColumn__a_mwP.GridColumn_gutter__wx4JI.GridColumn_small9__bUTRL > div > div > div:nth-child(6)",

        ],
        "image" : ["#maincontent > section > main > div.flex.flex-column.h-100 > div:nth-child(2) > div > div.GridColumn_gridColumn__a_mwP.GridColumn_gutter__wx4JI.GridColumn_small5__W1Pll > div > div > section > div:nth-child(1) > div.container.overflow-y-hidden.mb0"]
    }
}

# -------------------------------
# Helpers
# -------------------------------
def get_domain(url):
    host = urlparse(str(url)).netloc
    return host.replace("www.", "")

# First selector of "title" used as wait_selector to confirm page is rendered
DOMAIN_WAIT_SELECTOR = {
    domain: cfg["title"][0] for domain, cfg in DOMAIN_SELECTORS.items() if "title" in cfg
}

def get_domain_config(url):
    domain = get_domain(url)
    selectors, fetch_mode, wait_sel = None, "requests", None
    for key in DOMAIN_SELECTORS:
        if key in domain:
            selectors = DOMAIN_SELECTORS[key]
            wait_sel = DOMAIN_WAIT_SELECTOR.get(key)
            break
    for key in DOMAIN_FETCH_MODE:
        if key in domain:
            fetch_mode = DOMAIN_FETCH_MODE[key]
            break
    return selectors, fetch_mode, wait_sel

def fetch_html_requests(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    return response.text

STEALTH_INIT_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
    window.chrome = {runtime: {}};
"""

SCROLL_JS = """
    () => new Promise(resolve => {
        const step = 400, delay = 120;
        let y = 0;
        const tick = () => {
            window.scrollTo(0, y);
            y += step;
            if (y <= document.body.scrollHeight) setTimeout(tick, delay);
            else { window.scrollTo(0, 0); setTimeout(resolve, 1500); }
        };
        tick();
    })
"""

def _playwright_fetch(url, wait_selector=None):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            }
        )
        page = context.new_page()
        page.add_init_script(STEALTH_INIT_SCRIPT)
        try:
            page.goto(url, wait_until="networkidle", timeout=90000)
        except Exception:
            pass
        if wait_selector:
            try:
                page.wait_for_selector(wait_selector, timeout=10000)
            except Exception:
                pass
        page.evaluate(SCROLL_JS)
        page.wait_for_timeout(2000)
        html = page.content()
        browser.close()
    return html

def fetch_html_js(url):
    return _playwright_fetch(url)

def _zyte_browser_html(url):
    import base64
    response = requests.post(
        "https://api.zyte.com/v1/extract",
        auth=(ZYTE_API_KEY, ""),
        json={"url": url, "browserHtml": True},
        timeout=60,
    )
    if response.status_code == 200:
        return response.json().get("browserHtml", "")
    if response.status_code == 422:
        response = requests.post(
            "https://api.zyte.com/v1/extract",
            auth=(ZYTE_API_KEY, ""),
            json={"url": url, "httpResponseBody": True},
            timeout=60,
        )
        response.raise_for_status()
        return base64.b64decode(response.json()["httpResponseBody"]).decode("utf-8", errors="replace")
    response.raise_for_status()
    return ""

def fetch_html_zyte(url, wait_sel=None):
    """Try Playwright first (full JS render + scroll). Fall back to Zyte API if blocked."""
    try:
        html = _playwright_fetch(url, wait_sel)
        soup = BeautifulSoup(html, "html.parser")
        # If page looks like a bot-block / empty, fall back to Zyte
        body_text = soup.get_text(" ", strip=True)
        if len(body_text) > 500:
            return html
        print(f"[PLAYWRIGHT thin response, falling back to Zyte] {url}")
    except Exception as e:
        print(f"[PLAYWRIGHT failed, falling back to Zyte] {e}")
    return _zyte_browser_html(url)

def fetch_html(url, mode, wait_sel=None):
    if mode == "js":
        return _playwright_fetch(url, wait_sel)
    elif mode == "zyte":
        return fetch_html_zyte(url, wait_sel)
    return fetch_html_requests(url)

def scrape_to_json(url, selectors, fetch_mode, wait_sel=None):
    html = fetch_html(url, fetch_mode, wait_sel)
    soup = BeautifulSoup(html, "html.parser")
    result = {}

    for key, css_list in selectors.items():
        elements = None
        for selector in css_list:
            found = soup.select(selector)
            if found:
                elements = found
                break

        if not elements:
            result[key] = ""
            continue

        if key == "image":
            IMG_ATTRS = ["src", "data-src", "data-lazy-src", "data-original", "data-lazy", "data-image", "data-full-src"]
            img_idx = 0
            for el in elements:
                for img in el.select("img"):
                    src = next((img.get(a) for a in IMG_ATTRS if img.get(a) and not img.get(a, "").startswith("data:")), None)
                    if not src:
                        srcset = img.get("srcset", "")
                        src = srcset.split(",")[-1].strip().split(" ")[0] if srcset else None
                    if src:
                        result[f"image_{img_idx}"] = src
                        img_idx += 1
        elif key == "specifications":
            for el in elements:
                for li in el.select("li"):
                    parts = [s.strip() for s in li.get_text(" ", strip=True).split(":", 1)]
                    if len(parts) == 2 and parts[0]:
                        result[parts[0]] = parts[1]
                    elif parts[0]:
                        result[parts[0]] = ""
        else:
            result[key] = md("".join(str(el) for el in elements)).strip()

    return result

# -------------------------------
# Load CSV
# -------------------------------
df = pd.read_csv("item.csv")
competitor_data_col = []

for _, row in df.iterrows():
    comp_url = row["comp_url"]
    selectors, fetch_mode, wait_sel = get_domain_config(comp_url)

    if not selectors:
        print(f"[SKIP] No config for domain: {get_domain(comp_url)}")
        competitor_data_col.append("")
        continue

    try:
        result = scrape_to_json(comp_url, selectors, fetch_mode, wait_sel)
        competitor_data_col.append(json.dumps(result, ensure_ascii=False))
        print(f"[OK] {row['web_id']} — {get_domain(comp_url)} — mode:{fetch_mode}")
    except Exception as e:
        competitor_data_col.append("")
        print(f"[FAIL] {row['web_id']} — {e}")

# -------------------------------
# Output CSV
# -------------------------------
df["competitor_data"] = competitor_data_col

output_file = "item_output.csv"
if os.path.exists(output_file):
    os.remove(output_file)

df.to_csv(output_file, index=False)
print(f"\nSaved to {output_file}")
