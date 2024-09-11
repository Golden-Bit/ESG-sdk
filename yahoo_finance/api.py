import json
import os

import uvicorn
from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any, Optional
import random
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

from yahoo_finance_sdk import get_tickers, generate_data, GenerateDataRequest  # Importa le tue funzioni e classi

# Crea un'app FastAPI
app = FastAPI()

# Configurazione CORS per permettere tutte le origini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permetti tutte le origini
    allow_credentials=True,
    allow_methods=["*"],  # Permetti tutti i metodi (GET, POST, OPTIONS, ecc.)
    allow_headers=["*"],  # Permetti tutti gli headers
)

# Directory in cui si trovano i file di descrizione
DESCRIPTIONS_DIR = "extra_data/params_descriptions/"


def load_descriptions(blocco: str) -> Dict[str, Dict[str, str]]:
    """
    Carica le descrizioni dei parametri per un blocco specifico.

    :param blocco: Nome del blocco (esg_data, financials_data, etc.).
    :return: Un dizionario contenente le descrizioni in diverse lingue.
    """
    descriptions = {}
    for lang in ["it", "en"]:  # Si cercano i file per italiano e inglese
        filepath = os.path.join(DESCRIPTIONS_DIR, f"{blocco}-{lang}.json")
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                descriptions[lang] = json.load(f)
        else:
            descriptions[lang] = {}

    if not descriptions["it"] and not descriptions["en"]:
        raise FileNotFoundError(f"Descrizioni non trovate per il blocco {blocco}.")

    return descriptions


@app.post("/get_descriptions/{blocco}")
async def get_descriptions(
        blocco: str,
        params: Optional[List[str]] = None
):
    """
    Restituisce la descrizione dei parametri per il blocco e i parametri specificati.

    :param blocco: Nome del blocco (esg_data, financials_data, etc.).
    :param params: Lista opzionale di nomi di parametri. Se None, vengono restituiti tutti i parametri.
    :return: Un dizionario con le descrizioni in tutte le lingue disponibili.
    """
    try:
        descriptions = load_descriptions(blocco)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if params is None or not params:
        # Restituisci tutte le descrizioni
        return descriptions

    filtered_descriptions = {"it": {}, "en": {}}

    for param in params:
        for lang in ["it", "en"]:
            if param in descriptions[lang]:
                filtered_descriptions[lang][param] = descriptions[lang][param]
            else:
                filtered_descriptions[lang][param] = "Descrizione non disponibile."

    return filtered_descriptions


# Endpoint 1: Get tickers
@app.get("/tickers", response_model=List[Dict[str, Any]])
async def tickers(file_path: str = "tickers_data.json"):
    """
    Ottiene tutti i tickers dal file JSON specificato.
    :param file_path: Percorso del file JSON contenente i dati dei tickers.
    :return: Lista di dizionari con ticker, company_name, sector, market_cap.
    """
    try:
        tickers_data = get_tickers(file_path)
        return tickers_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

'''
# Endpoint 2: Filter tickers
@app.get("/tickers/filter", response_model=List[Dict[str, Any]])
async def filter_tickers(...)

    return
'''


# Endpoint 3: Get data by ticker
@app.post("/tickers/{ticker}/data", response_model=Dict[str, Any])
async def data_by_ticker(request: GenerateDataRequest):
    """
    Ottiene i dati per un ticker specifico.

    :return: Dati per il ticker specifico.
    """
    try:
        output_data = generate_data(request)
        return output_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

'''
# Endpoint 4: Filter data
@app.get("/tickers/data/filter", response_model=List[Dict[str, Any]])
async def filter_data(...)
        
        return
'''


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8301)
