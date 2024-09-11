import uuid
from fastapi import FastAPI, HTTPException
from fastapi import FastAPI, Query
from typing import List, Optional
import os
import json
import pandas as pd
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

from eurostat_sdk import EurostatAPI  # Importa la classe EurostatAPI dal file dove è implementata

app = FastAPI()

# Configurazione CORS per permettere tutte le origini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permetti tutte le origini
    allow_credentials=True,
    allow_methods=["*"],  # Permetti tutti i metodi (GET, POST, OPTIONS, ecc.)
    allow_headers=["*"],  # Permetti tutti gli headers
)

# Inizializza l'oggetto EurostatAPI
eurostat_api = EurostatAPI()


# Modello per la richiesta di filtraggio
class GenerateDataRequest(BaseModel):
    dataset_id: Optional[str] = None
    geo: Optional[str] = None
    unit: Optional[str] = None
    time_range: Optional[List[str]] = None
    indicators: Optional[List[str]] = None


# Percorsi dei file JSON
IT_FILE_PATH = "extra_data/params_descriptions/indicators-it.json"
EN_FILE_PATH = "extra_data/params_descriptions/indicators-en.json"


# Funzione per caricare i dati dal file JSON
def load_indicators(language: str):
    if language == "it":
        file_path = IT_FILE_PATH
    elif language == "en":
        file_path = EN_FILE_PATH
    else:
        raise HTTPException(status_code=400, detail="Lingua non supportata. Utilizzare 'it' o 'en'.")

    try:
        with open(file_path, "r") as f:
            indicators = json.load(f)
        return indicators
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File degli indicatori non trovato.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Errore nel caricamento del file: {str(e)}")


# Endpoint per ottenere tutti gli indicatori con nome, descrizione e id
@app.get("/indicators/descriptions", response_model=List[dict])
def get_indicators_descriptions(language: str = "it"):
    """
    Ottieni tutti gli indicatori con i campi id, nome e descrizione.
    :param language: 'it' per italiano, 'en' per inglese.
    :return: Lista di indicatori con campi id, nome e descrizione.
    """
    indicators = load_indicators(language)

    # Crea una lista di oggetti con campi id, nome e descrizione
    result = []
    for indicator in indicators:
        result.append({
            "id": indicator.get("indicator_id"),
            "nome": indicator.get("indicator_name"),
            "descrizione": indicator.get("indicator_description")
        })

    return result


@app.get("/indicators/")
async def get_indicators(dataset_id: str = Query(..., description="ID del dataset Eurostat")):
    """
    Restituisce una lista di indicatori per il dataset scelto.
    """
    dataset = eurostat_api.get_dataset(dataset_id, format_type='json', cache=False)

    dataset = {"data": dataset}

    if not dataset:
        return {"error": "Dataset non trovato"}

    # Estrae gli indicatori dal dataset JSON
    if 'dimension' in dataset['data'] and 'na_item' in dataset['data']['dimension']:
        indicators = dataset['data']['dimension']['na_item']['category']['label']
        return {"indicators": indicators}
    else:
        return {"error": "Dimensioni non trovate nel dataset"}


@app.post("/generate_data/")
async def generate_data(request: GenerateDataRequest):
    eurostat = EurostatAPI()
    dataset_id = request.dataset_id
    params = {'geo': request.geo, 'unit': request.unit}
    indicators = request.indicators
    time_range = request.time_range

    _id = str(uuid.uuid4())

    try:
        os.mkdir(f"../output")
    except Exception as e:
        pass

    try:
        os.mkdir(f"output/output_{_id}")
    except Exception as e:
        pass

    request_json = json.loads(request.model_dump_json())

    with open(f"output/output_{_id}/input_request.json", 'w') as f:
        json.dump(request_json, f, indent=2)

    dataset = eurostat.get_dataset(dataset_id, params=params, format_type='json', cache=True,
                                   cache_file=f"output/output_{_id}/raw_data.json")
    dataset = {"data": dataset}

    # Processare il dataset e salvarlo in JSON e CSV (con delimitatore ';')
    processed_dataset = eurostat.process_dataset_to_json(dataset, output_dir=f"output/output_{_id}/processed_data", save_separate_tables=True, save_csv=True,
                                                         csv_delimiter=';')

    for key in processed_dataset:
        processed_dataset[key] = json.loads(processed_dataset[key])

    if indicators:
        processed_dataset = {k: v for k, v in processed_dataset.items() if k in indicators}

    if time_range:
        for key in processed_dataset:
            processed_dataset[key] = [data for data in processed_dataset[key] if data["time"] in time_range]

    return {
        "_id": _id,
        "raw_data": dataset,
        "processed_data": processed_dataset
    }


@app.get("/dataset/parameters/")
async def get_dataset_parameters(dataset_id: str = Query(..., description="ID del dataset Eurostat")):
    """
    Restituisce una lista di parametri disponibili (geo, unit, time) per un dataset scelto.
    """
    dataset = eurostat_api.get_dataset(dataset_id, format_type='json', cache=False)

    if not dataset:
        return {"error": "Dataset non trovato"}

    dataset = {"data": dataset}

    dimensions = dataset['data']['dimension']

    geo_options = dimensions['geo']['category']['label'] if 'geo' in dimensions else {}
    unit_options = dimensions['unit']['category']['label'] if 'unit' in dimensions else {}
    time_options = dimensions['time']['category']['label'] if 'time' in dimensions else {}

    return {
        "geo_options": geo_options,
        "unit_options": unit_options,
        "time_options": time_options
    }


# Avvio dell'applicazione FastAPI
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8201)
