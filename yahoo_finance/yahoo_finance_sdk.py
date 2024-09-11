import json
import os
import random
from typing import Optional, List, Dict, Any, Literal

import yfinance as yf
import pandas as pd
from pydantic import BaseModel


class GenerateDataRequest(BaseModel):
    ticker: Optional[str] = "AAPL"
    data_id: Optional[Literal["esg_data", "company_data", "financials_data", "dividends_data", "stock_history"]] = "esg_data"
    data_params: Optional[Dict[str, Any]] = {}


class ESGDataFetcher:
    """
    Classe per ottenere tutte le informazioni aziendali, inclusi i dati ESG,
    bilanci e altre metriche finanziarie, utilizzando yfinance.
    """

    def __init__(self, ticker, output_dir="output"):
        """
        Inizializza la classe con il ticker dell'azienda e la directory di output.

        :param ticker: Simbolo azionario (ticker) dell'azienda, es. 'AAPL' per Apple.
        :param output_dir: Directory in cui salvare i file JSON.
        """
        self.ticker = ticker
        self.output_dir = "/".join([output_dir, self.ticker])
        self.company = None
        self.fetch_company_data()

        # Crea la directory se non esiste
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def fetch_company_data(self):
        """
        Ottiene i dati aziendali usando yfinance.
        """
        try:
            self.company = yf.Ticker(self.ticker)
        except Exception as e:
            print(f"Errore nel recupero dei dati per {self.ticker}: {e}")

    def save_json(self, data, filename):
        """
        Salva i dati in un file JSON formattato.

        :param data: I dati da salvare.
        :param filename: Nome del file in cui salvare i dati.
        """
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Dati salvati in {filepath}")
        except Exception as e:
            print(f"Errore nel salvataggio del file {filename}: {e}")

        # Ricarica e restituisce il contenuto del file JSON
        return self.load_json(filepath)

    def load_json(self, filepath):
        """
        Carica il contenuto di un file JSON.

        :param filepath: Il percorso del file JSON da caricare.
        :return: Il contenuto del file JSON.
        """
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Errore nel caricamento del file {filepath}: {e}")
            return None

    def save_dataframe_to_json(self, df, filename):
        """
        Salva un DataFrame in formato JSON formattato.

        :param df: DataFrame da salvare.
        :param filename: Nome del file in cui salvare i dati.
        """
        filepath = os.path.join(self.output_dir, filename)
        try:
            df.to_json(filepath, orient='columns', indent=4)
            print(f"Dati DataFrame salvati in {filepath}")
        except Exception as e:
            print(f"Errore nel salvataggio del DataFrame {filename}: {e}")

        # Ricarica e restituisce il contenuto del file JSON
        return self.load_json(filepath)

    def get_esg_data(self):
        """
        Ottiene i dati ESG dell'azienda, se disponibili, e salva in formato JSON.

        :return: Il contenuto del file JSON salvato con i dati ESG.
        """
        try:
            esg_data = self.company.sustainability
            if esg_data is not None and not esg_data.empty:
                return self.save_dataframe_to_json(esg_data, f"{self.ticker}_esg_data.json")
            else:
                return "Dati ESG non disponibili per questo ticker."
        except Exception as e:
            return f"Errore nel recupero dei dati ESG: {e}"

    def get_company_info(self):
        """
        Ottiene informazioni generali sull'azienda, come nome, settore e paese, e salva in JSON.

        :return: Il contenuto del file JSON salvato con le informazioni aziendali.
        """
        try:
            info = self.company.info
            return self.save_json(info, f"{self.ticker}_company_info.json")
        except Exception as e:
            return f"Errore nel recupero delle informazioni aziendali: {e}"

    def get_financials(self, year="latest"):
        """
        Ottiene i dati finanziari dell'azienda, filtrando per l'anno specificato, e salva in JSON.

        :param year: L'anno da filtrare, default 'latest' per ottenere i dati più recenti.
        :return: Un dizionario contenente i contenuti dei file JSON salvati con bilanci, conto economico e flussi di cassa.
        """
        try:
            financials = {
                "bilancio": self._filter_by_year(self.company.balance_sheet, year),
                "conto_economico": self._filter_by_year(self.company.financials, year),
                "flussi_di_cassa": self._filter_by_year(self.company.cashflow, year)
            }
            json_financials = {}
            for key, value in financials.items():
                if True: #isinstance(value, pd.DataFrame):
                    json_financials[key] = self.save_dataframe_to_json(value, f"{self.ticker}_{key}.json")
            return json_financials
        except Exception as e:
            return f"Errore nel recupero dei dati finanziari: {e}"

    def _filter_by_year(self, dataframe, year):
        """
        Filtra un DataFrame per l'anno più recente o un anno specifico.

        :param dataframe: Il DataFrame da filtrare.
        :param year: L'anno da filtrare, 'latest' per ottenere l'anno più recente.
        :return: Un DataFrame filtrato per l'anno specificato.
        """
        if dataframe is None or dataframe.empty:
            return "Dati non disponibili"

        if year == "latest":
            return dataframe.iloc[:, 0]  # Restituisce la colonna più recente
        else:
            try:
                return dataframe[year]
            except KeyError:
                return f"Anno {year} non trovato nei dati finanziari."

    def get_dividends(self):
        """
        Ottiene la storia dei dividendi dell'azienda e salva in JSON.

        :return: Il contenuto del file JSON salvato con la storia dei dividendi.
        """
        try:
            dividends = self.company.dividends
            if not dividends.empty:
                return self.save_dataframe_to_json(dividends, f"{self.ticker}_dividends.json")
            else:
                return "Nessun dividendo disponibile per questo ticker."
        except Exception as e:
            return f"Errore nel recupero della storia dei dividendi: {e}"

    def get_stock_history(self, period="1y"):
        """
        Ottiene la storia dei prezzi delle azioni dell'azienda e salva in JSON.

        :param period: Periodo di tempo per la storia dei prezzi, ad esempio '1y', '5y', 'max'.
        :return: Il contenuto del file JSON salvato con la storia dei prezzi delle azioni.
        """
        try:
            stock_history = self.company.history(period=period)
            if not stock_history.empty:
                return self.save_dataframe_to_json(stock_history, f"{self.ticker}_stock_history.json")
            else:
                return f"Nessun dato di prezzi per il periodo specificato: {period}."
        except Exception as e:
            return f"Errore nel recupero della storia dei prezzi: {e}"


# Esempio di utilizzo dell'SDK
def generate_data(request: GenerateDataRequest):

    ticker = request.ticker  # Ticker per Apple
    data_id = request.data_id
    data_params = request.data_params

    esg_fetcher = ESGDataFetcher(ticker)

    getters = {
        "esg_data": esg_fetcher.get_esg_data,
        "company_data": esg_fetcher.get_company_info,
        "financials_data": esg_fetcher.get_financials,
        "dividends_history": esg_fetcher.get_dividends,
        "stock_history": esg_fetcher.get_stock_history,
    }

    '''
    # Ottieni i dati ESG
    esg_data = esg_fetcher.get_esg_data()

    # Ottieni informazioni generali sull'azienda
    company_info = esg_fetcher.get_company_info()

    # Ottieni dati finanziari filtrati per l'anno più recente
    financials = esg_fetcher.get_financials(year="latest")

    # Ottieni la storia dei dividendi
    dividends = esg_fetcher.get_dividends()

    # Ottieni la storia dei prezzi delle azioni (ultimi 2 anni)
    stock_history = esg_fetcher.get_stock_history(period="max")

    output_data = {
        "esg_data": esg_data,
        "company_info": company_info,
        "financials_data": financials,
        "dividends_history": dividends,
        "stock_history": stock_history,
    }'''

    output_data = getters[data_id](**data_params)

    return output_data


def get_tickers(file_path: str) -> List[Dict[str, str]]:
    """
    Estrae i ticker, nome dell'azienda, settore e market cap da un file JSON.

    :param file_path: Percorso del file JSON che contiene i dati aziendali.
    :return: Una lista di dizionari contenenti 'ticker', 'company_name', 'sector' e 'market_cap'.
    """
    try:
        # Leggi il file JSON e carica i dati
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Estrai i campi ticker, company_name, sector e market_cap
        extracted_data = [
            {
                'ticker': item.get('ticker', 'N/A'),
                'company_name': item.get('company_name', 'N/A'),
                'sector': item.get('sector', 'N/A'),
                'market_cap': item.get('market_cap', 'N/A')
            }
            for item in data if 'ticker' in item
        ]

        return extracted_data

    except FileNotFoundError:
        print(f"Errore: File non trovato - {file_path}")
        return []

    except json.JSONDecodeError:
        print(f"Errore: Formato JSON non valido - {file_path}")
        return []

    except Exception as e:
        print(f"Errore imprevisto durante l'estrazione dei dati: {e}")
        return []


# Test con selezione casuale di ticker
if __name__ == "__main__":
    # Carica i ticker da un file JSON
    ticker_file_path = 'tickers_data.json'  # Assumi che questo sia il file JSON che contiene i ticker
    with open(ticker_file_path, 'r') as f:
        tickers_data = json.load(f)

    # Estrai i ticker e seleziona un numero casuale di ticker per il test
    tickers = [item['ticker'] for item in tickers_data]
    random_tickers = random.sample(tickers, 5)  # Seleziona 5 ticker a caso

    # Testa la funzione get_stock_history per ciascun ticker selezionato
    for ticker in random_tickers:
        print(f"\n\nTest per il ticker: {ticker}")
        request = GenerateDataRequest(ticker=ticker, data_id="financials_data", data_params={})
        stock_history = generate_data(request)
        print(json.dumps(stock_history, indent=2))
