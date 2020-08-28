from bs4 import BeautifulSoup
from fileUtils import fileDownload

# function that returns TransMonEE list of countries from url web
def get_countries(url_transmonee):
    page = fileDownload.api_request(url_transmonee)
    # parse HTML
    page_html = BeautifulSoup(page.content, "html.parser")
    country_soup = page_html.findAll("div", {"class": "left-nav"})
    # get text from html lines into list of countries
    country_names = [
        line.get_text(strip=True).lower() for line in country_soup[0].findAll("a")
    ]
    return country_names
