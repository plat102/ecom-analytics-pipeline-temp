from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://www.glamira.de/glamira-ring-griseus.html")

    soup = BeautifulSoup(page.content(), 'html.parser')

    # Test og:title
    og_title = soup.find('meta', property='og:title')
    print("og:title:", og_title['content'] if og_title else "NOT FOUND")

    # Test h1 span fallback
    h1_span = soup.select_one('h1 span')
    print("h1 span:", h1_span.get_text(strip=True) if h1_span else "NOT FOUND")

    browser.close()