from selenium import webdriver
from selenium.webdriver import Chrome
from time import sleep

opcoes = webdriver.ChromeOptions()

opcoes.add_experimental_option(
    'prefs', {
        "download.default_directory": "C:\\download_pdf",
        "download.directory_upgrade": True,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally:": True

    }
)
opcoes.add_argument("--headless")

def encontra_link(driver, link):
    elementos = driver.find_elements_by_tag_name('a')
    lista = []
    for elemento in elementos:
        if link in elemento.get_attribute('href'):
            lista.append(elemento.get_attribute('href'))
    return(lista)

def baixapdf(driver,link):
    driver.get(link)
    sleep(5)


driver = Chrome(options=opcoes)
driver.get("http://www.imea.com.br/imea-site/relatorios-mercado-detalhe?c=4&s=8")

sleep(2)

elemento_link = encontra_link(driver,'amazonaws')
print(len(elemento_link))
for link in elemento_link:
    baixa = baixapdf(driver,link)

driver.quit()