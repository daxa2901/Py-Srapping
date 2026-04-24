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
    "diningroomsoutlet.com" :"zyte",
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
        "image" : ["#maincontent > main > section > div.flex.flex-column.h-100 > div:nth-child(2) > div > div.GridColumn_gridColumn__a_mwP.GridColumn_gutter__wx4JI.GridColumn_small5__W1Pll > div > div > section > div:nth-child(1) > div.container.overflow-y-hidden.mb0"]
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

# First selector of "image" used to wait until image container is in DOM
DOMAIN_IMAGE_SELECTOR = {
    domain: cfg["image"][0] for domain, cfg in DOMAIN_SELECTORS.items() if "image" in cfg
}

def get_domain_config(url):
    domain = get_domain(url)
    selectors, fetch_mode, wait_sel, img_sel = None, "requests", None, None
    for key in DOMAIN_SELECTORS:
        if key in domain:
            selectors = DOMAIN_SELECTORS[key]
            wait_sel = DOMAIN_WAIT_SELECTOR.get(key)
            img_sel = DOMAIN_IMAGE_SELECTOR.get(key)
            break
    for key in DOMAIN_FETCH_MODE:
        if key in domain:
            fetch_mode = DOMAIN_FETCH_MODE[key]
            break
    return selectors, fetch_mode, wait_sel, img_sel

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

EXPAND_JS = """
    () => {
        const triggers = document.querySelectorAll(
            '[data-toggle="collapse"], .accordion-header, .accordion-button, ' +
            '.tab-label, [aria-expanded="false"], .expand-collapse-content button, ' +
            '.Collapse_collapse__Ja6XL button, details:not([open]) summary'
        );
        triggers.forEach(el => { try { el.click(); } catch(e) {} });
    }
"""

SCROLL_JS = """
    async () => {
        await new Promise(resolve => {
            let lastHeight = 0;
            let unchangedCount = 0;
            const step = 300, delay = 150;
            let y = 0;
            const tick = () => {
                window.scrollTo(0, y);
                y += step;
                const currentHeight = document.body.scrollHeight;
                if (y <= currentHeight) {
                    if (currentHeight === lastHeight) unchangedCount++;
                    else { unchangedCount = 0; lastHeight = currentHeight; }
                    setTimeout(tick, delay);
                } else {
                    window.scrollTo(0, 0);
                    setTimeout(resolve, 2000);
                }
            };
            tick();
        });
    }
"""

WAIT_FOR_IMAGES_JS = """
    async (imgSelector) => {
        const container = document.querySelector(imgSelector);
        if (!container) return;
        const imgs = Array.from(container.querySelectorAll('img'));
        await Promise.all(imgs.map(img => new Promise(resolve => {
            if (img.complete && img.naturalWidth > 0) return resolve();
            img.addEventListener('load', resolve);
            img.addEventListener('error', resolve);
            setTimeout(resolve, 5000);
        })));
    }
"""

def _playwright_fetch(url, wait_selector=None, img_selector=None):
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
        page.evaluate(EXPAND_JS)
        page.wait_for_timeout(500)
        page.evaluate(SCROLL_JS)
        # Wait for lazy-triggered network requests to settle
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        # Scroll image container into view and wait for all images to fully load
        if img_selector:
            try:
                page.wait_for_selector(img_selector, timeout=10000)
                page.evaluate(f"document.querySelector('{img_selector}')?.scrollIntoView({{behavior:'smooth',block:'center'}})")
                page.wait_for_timeout(1000)
                page.evaluate(WAIT_FOR_IMAGES_JS, img_selector)
            except Exception:
                pass
        page.wait_for_timeout(1500)
        html = page.content()
        browser.close()
    return html

def fetch_html_js(url, img_selector=None):
    return _playwright_fetch(url, img_selector=img_selector)

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

def is_captcha_page(html: str) -> bool:
    """Detect AWS WAF CAPTCHA / bot-block pages."""
    return any(marker in html for marker in (
        "awswaf",
        "AwsWafIntegration",
        "CaptchaScript.renderCaptcha",
        "Let's confirm you are human",
        "challenge.js",
    ))

def fetch_html_zyte(url, wait_sel=None, img_sel=None):
    """Try Playwright first (full JS render + scroll). Fall back to Zyte API if blocked."""
    try:
        html = _playwright_fetch(url, wait_sel, img_selector=img_sel)
        if is_captcha_page(html):
            print(f"[PLAYWRIGHT CAPTCHA detected, falling back to Zyte] {url}")
        else:
            soup = BeautifulSoup(html, "html.parser")
            body_text = soup.get_text(" ", strip=True)
            if len(body_text) > 500:
                return html
            print(f"[PLAYWRIGHT thin response, falling back to Zyte] {url}")
    except Exception as e:
        print(f"[PLAYWRIGHT failed, falling back to Zyte] {e}")
    return _zyte_browser_html(url)

def fetch_html(url, mode, wait_sel=None, img_sel=None):
    if mode == "js":
        return _playwright_fetch(url, wait_sel, img_selector=img_sel)
    elif mode == "zyte":
        return fetch_html_zyte(url, wait_sel, img_sel=img_sel)
    return fetch_html_requests(url)

def scrape_to_json(url, selectors, fetch_mode, wait_sel=None, img_sel=None):
    html = fetch_html(url, fetch_mode, wait_sel, img_sel)
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
            IMG_ATTRS = [
                "data-zoom-image", "data-large-image", "data-full-src",
                "data-src", "data-lazy-src", "data-original", "data-lazy",
                "data-image", "src"
            ]
            img_idx = 0
            for el in elements:
                for img in el.select("img"):
                    src = next(
                        (img.get(a) for a in IMG_ATTRS
                         if img.get(a)
                         and not img.get(a, "").startswith("data:")
                         and img.get(a, "").strip()),
                        None
                    )
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

OUTPUT_BASE = "output"

# -------------------------------
# Prepare — split item.csv domain-wise
# -------------------------------
def prepare(input_csv="item.csv"):
    df = pd.read_csv(input_csv)
    domain_files = {}

    for _, row in df.iterrows():
        domain = get_domain(str(row["comp_url"]))
        if not domain:
            continue
        folder = os.path.join(OUTPUT_BASE, domain)
        os.makedirs(folder, exist_ok=True)
        domain_files.setdefault(domain, []).append(row)

    for domain, rows in domain_files.items():
        folder = os.path.join(OUTPUT_BASE, domain)
        path = os.path.join(folder, f"{domain}.csv")
        pd.DataFrame(rows).to_csv(path, index=False)
        print(f"[PREPARE] {domain} — {len(rows)} items → {path}")

    return list(domain_files.keys())

# -------------------------------
# Process — scrape each domain file
# -------------------------------
def process(domain):
    input_path = os.path.join(OUTPUT_BASE, domain, f"{domain}.csv")
    if not os.path.exists(input_path):
        print(f"[PROCESS] File not found: {input_path}")
        return

    df = pd.read_csv(input_path)
    competitor_data_col = []

    for _, row in df.iterrows():
        comp_url = row["comp_url"]
        selectors, fetch_mode, wait_sel, img_sel = get_domain_config(comp_url)

        if not selectors:
            print(f"[SKIP] No config for domain: {get_domain(comp_url)}")
            competitor_data_col.append("")
            continue

        try:
            result = scrape_to_json(comp_url, selectors, fetch_mode, wait_sel, img_sel)
            competitor_data_col.append(json.dumps(result, ensure_ascii=False))
            print(f"[OK] {row['web_id']} — {domain} — mode:{fetch_mode}")
        except Exception as e:
            competitor_data_col.append("")
            print(f"[FAIL] {row['web_id']} — {e}")

    df["competitor_data"] = competitor_data_col
    output_path = os.path.join(OUTPUT_BASE, domain, f"{domain}_v1.csv")
    df.to_csv(output_path, index=False)
    print(f"[PROCESS] Done — {domain} → {output_path}")

# -------------------------------
# MergeCsv — merge all *_v1.csv into item_output.csv
# -------------------------------
def mergeCsv():
    frames = []
    for domain in os.listdir(OUTPUT_BASE):
        v1_path = os.path.join(OUTPUT_BASE, domain, f"{domain}_v1.csv")
        if os.path.exists(v1_path):
            frames.append(pd.read_csv(v1_path))
            print(f"[MERGE] Including {v1_path}")

    if not frames:
        print("[MERGE] No v1 files found.")
        return

    merged = pd.concat(frames, ignore_index=True)
    output_file = "item_output.csv"
    if os.path.exists(output_file):
        os.remove(output_file)
    merged.to_csv(output_file, index=False)
    print(f"[MERGE] Saved {len(merged)} rows → {output_file}")

# -------------------------------
# Entry point
# -------------------------------
if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "all"

    if cmd == "prepare":
        domains = prepare()
        # Print domain list as JSON for workflow matrix
        print("DOMAINS=" + json.dumps(domains))

    elif cmd == "process":
        domain = sys.argv[2]
        process(domain)

    elif cmd == "merge":
        mergeCsv()

    else:
        domains = prepare()
        for domain in domains:
            process(domain)
        mergeCsv()
