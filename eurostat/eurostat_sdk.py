import os
import requests
import pandas as pd
import json
import time
import matplotlib.pyplot as plt
import asyncio
import aiohttp
from requests.exceptions import HTTPError, Timeout


class EurostatAPI:
    BASE_URL_JSON = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    BASE_URL_SDMX = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/"
    BASE_URL_CATALOGUE = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/v2.0/"

    def __init__(self):
        pass

    def _make_request(self, url, params=None, timeout=10, compressed=True):
        """
        Funzione interna per gestire le chiamate API, con gestione degli errori.
        Gestisce dati compressi (gzip) se richiesto.
        :param url: L'URL dell'endpoint API.
        :param params: Parametri opzionali per la query.
        :param timeout: Timeout per la richiesta (default 10 secondi).
        :param compressed: Se True, invia richieste accettando dati compressi gzip.
        :return: Risultato della richiesta in formato JSON.
        """
        headers = {}
        if compressed:
            headers['Accept-Encoding'] = 'gzip'

        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except HTTPError as http_err:
            print(f"Errore HTTP: {http_err}")
        except Timeout as timeout_err:
            print(f"Timeout: {timeout_err}")
        except Exception as err:
            print(f"Errore generico: {err}")
        return None

    def _cache_response(self, cache_file, data, timeout=3600):
        """
        Salva la risposta in cache.
        :param cache_file: Nome del file di cache.
        :param data: Dati da salvare.
        :param timeout: Timeout per considerare i dati freschi (in secondi).
        """
        current_time = time.time()
        cache_data = {'data': data, 'timestamp': current_time}
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)

    def _load_cache(self, cache_file, timeout=3600):
        """
        Carica i dati dalla cache se validi.
        :param cache_file: Nome del file di cache.
        :param timeout: Tempo in secondi per considerare i dati validi.
        :return: Dati in cache se validi, altrimenti None.
        """
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                if time.time() - cache_data['timestamp'] < timeout:
                    return cache_data['data']
        return None

    def _explore_json(self, dataset_json):
        """
        Esplora il JSON ricevuto dall'API per determinare la struttura.
        :param dataset_json: Dataset JSON ricevuto dall'API.
        """
        print(json.dumps(dataset_json, indent=4))  # Stampa il JSON in modo leggibile

    def get_dataset(self, dataset_id, params=None, format_type='json', cache=False, cache_file="cache.json"):
        """
        Scarica un dataset specifico da Eurostat in formato JSON-stat o SDMX.
        :param dataset_id: ID del dataset Eurostat (es. 'nama_10_gdp').
        :param params: Parametri di query per filtrare i dati.
        :param format_type: Formato del dataset ('json' per JSON-stat, 'sdmx' per SDMX).
        :param cache: Se True, carica/salva i dati dalla cache.
        :param cache_file: Nome del file di cache.
        :return: Dataset in formato JSON o XML.
        """
        if cache:
            cached_data = self._load_cache(cache_file)
            if cached_data:
                print(f"Caricato da cache {cache_file}")
                return cached_data

        if format_type == 'json':
            url = f"{self.BASE_URL_JSON}{dataset_id}"
        elif format_type == 'sdmx':
            url = f"{self.BASE_URL_SDMX}{dataset_id}/1.0"
        else:
            raise ValueError(f"Formato {format_type} non supportato.")

        dataset = self._make_request(url, params=params)
        if dataset:
            print(f"Dataset '{dataset_id}' scaricato con successo!")
            if cache:
                self._cache_response(cache_file, dataset)
            return dataset
        else:
            print(f"Errore: Impossibile scaricare il dataset {dataset_id}")
            return None

    def process_dataset_to_json(self, dataset_json, output_dir="output", save_separate_tables=False, save_csv=False,
                                csv_delimiter=","):
        """
        Funzione per processare un dataset in formato JSON e generare una rappresentazione per ogni indicatore.
        Salva i risultati finali in un file JSON e, opzionalmente, salva ogni tabella come file JSON e/o CSV separato.

        :param dataset_json: Il dataset in formato JSON da processare
        :param output_dir: La directory in cui salvare i file di output (default: "output")
        :param save_separate_tables: Se True, salva ogni tabella separatamente come file JSON (default: False)
        :param save_csv: Se True, salva ogni tabella separatamente come file CSV (default: False)
        :param csv_delimiter: Delimitatore da utilizzare nei file CSV (default: ',')
        :return: None
        """
        # Creare la directory di output se non esiste
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Verifica se la chiave 'dimension' esiste nel dataset
        if 'dimension' in dataset_json['data']:
            print("La chiave 'dimension' esiste nel dataset.")

            # Estrarre le etichette delle dimensioni
            time_labels = dataset_json['data']['dimension']['time']['category']['label']
            na_item_labels = dataset_json['data']['dimension']['na_item']['category']['label']
            geo_labels = dataset_json['data']['dimension']['geo']['category']['label']
            unit_labels = dataset_json['data']['dimension']['unit']['category']['label']

            # Dizionario per contenere i DataFrame per ogni indicatore
            indicator_dfs = {}

            # Estrarre i valori effettivi
            values = dataset_json['data']['value']

            # Creare un DataFrame per le dimensioni
            data = []
            for value_key, value in values.items():
                idx = int(value_key)

                # Modifica del calcolo degli indici per garantire che siano mappati correttamente
                time_key = list(time_labels.keys())[idx % len(time_labels)]
                na_item_key = list(na_item_labels.keys())[(idx // len(time_labels)) % len(na_item_labels)]

                # Verificare che le chiavi calcolate siano presenti
                if time_key in time_labels:
                    time = time_labels[time_key]
                else:
                    print(f"Chiave {time_key} non trovata in time_labels")
                    continue

                if na_item_key in na_item_labels:
                    na_item = na_item_labels[na_item_key]
                else:
                    print(f"Chiave {na_item_key} non trovata in na_item_labels")
                    continue

                # Cerca il valore del geo e dell'unit utilizzando le chiavi fornite dall'utente, con un fallback a 'Unknown Geo' e 'Unknown Unit'
                geo = list(geo_labels.keys())[0]
                unit = list(unit_labels.keys())[0]

                # Aggiungere i dati per il DataFrame
                data.append({
                    'time': time,
                    'geo': geo,
                    'unit': unit,
                    'value': value
                })

                # Se l'indicatore non è già presente nel dizionario, creiamo una lista per esso
                if na_item not in indicator_dfs:
                    indicator_dfs[na_item] = []

                # Aggiungere la riga per questo indicatore
                indicator_dfs[na_item].append({
                    'time': time,
                    'geo': geo,
                    'unit': unit,
                    'value': value
                })

            # Convertire i dati dei singoli indicatori in DataFrame e poi in formato JSON o CSV
            json_result = {}
            for indicator, records in indicator_dfs.items():
                df = pd.DataFrame(records)
                json_result[indicator] = df.to_json(orient='records')  # Convertire il DataFrame in JSON

                # Se richiesto, salva ogni tabella separatamente in JSON
                if save_separate_tables:
                    output_table_path = os.path.join(output_dir, f"{indicator.replace(' ', '_')}.json")
                    with open(output_table_path, 'w', encoding='utf-8') as table_file:
                        json.dump(json.loads(json_result[indicator]), table_file, indent=4)
                    print(f"Tabella JSON salvata per {indicator}: {output_table_path}")

                # Se richiesto, salva ogni tabella separatamente in CSV
                if save_csv:
                    output_csv_path = os.path.join(output_dir, f"{indicator.replace(' ', '_')}.csv")
                    df.to_csv(output_csv_path, sep=csv_delimiter, index=False)
                    print(f"Tabella CSV salvata per {indicator}: {output_csv_path}")

            # Salva il JSON finale contenente tutte le tabelle
            output_json_path = os.path.join(output_dir, "final_output.json")
            with open(output_json_path, 'w', encoding='utf-8') as output_file:
                json.dump(json_result, output_file, indent=4)
            print(f"Output JSON finale salvato in: {output_json_path}")
            return json_result

        else:
            print("La chiave 'dimension' non esiste nel dataset.")

    async def _make_async_request(self, url, params=None, timeout=10):
        """
        Effettua una richiesta asincrona.
        :param url: L'URL dell'endpoint API.
        :param params: Parametri opzionali per la query.
        :param timeout: Timeout per la richiesta (default 10 secondi).
        :return: Risultato della richiesta in formato JSON.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=timeout) as response:
                return await response.json()

    async def get_dataset_async(self, dataset_id, params=None):
        """
        Scarica un dataset in modo asincrono.
        :param dataset_id: ID del dataset.
        :param params: Parametri di query.
        """
        url = f"{self.BASE_URL_JSON}{dataset_id}"
        return await self._make_async_request(url, params=params)


# Esempio di utilizzo dell'SDK e funzioni
if __name__ == "__main__":
    eurostat = EurostatAPI()

    dataset_id = "nama_10_gdp"
    params = {'geo': 'IT', 'unit': 'CP_MEUR'}

    dataset = eurostat.get_dataset(dataset_id, params=params, format_type='json', cache=True,
                                   cache_file="gdp_cache.json")
    dataset = {"data": dataset}

    # Processare il dataset e salvarlo in JSON e CSV (con delimitatore ';')
    eurostat.process_dataset_to_json(dataset, output_dir="output", save_separate_tables=True, save_csv=True,
                                     csv_delimiter=';')
