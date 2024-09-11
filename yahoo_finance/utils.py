import json
from typing import List, Dict


def extract_data_from_file(file_path):
    """
    Estrae tutti i campi (ticker, nome azienda, settore, capitalizzazione di mercato, entrate)
    da un file di testo che contiene dati formattati e restituisce una lista di dizionari.

    :param file_path: Percorso del file di testo.
    :return: Una lista di dizionari contenenti i dati estratti.
    """
    data_list = []

    try:
        # Apri e leggi il file di testo
        with open(file_path, 'r') as file:
            lines = file.readlines()

        # Processa ogni riga e estrae i campi necessari
        for line in lines:
            # Divide la riga in base ai tabulatori
            line_parts = line.split('\t')

            # Assicurati che la riga abbia almeno 5 elementi (ticker, nome, settore, market cap, entrate)
            if len(line_parts) >= 5:
                ticker = line_parts[0].strip()  # Ticker
                company_name = line_parts[1].strip()  # Nome dell'azienda
                sector = line_parts[2].strip()  # Settore
                market_cap = line_parts[3].strip()  # Capitalizzazione di mercato
                revenue = line_parts[4].strip()  # Entrate

                # Crea un dizionario con i dati estratti
                company_data = {
                    "ticker": ticker,
                    "company_name": company_name,
                    "sector": sector,
                    "market_cap": market_cap,
                    "revenue": revenue
                }

                # Aggiungi il dizionario alla lista
                data_list.append(company_data)

        return data_list

    except FileNotFoundError:
        print(f"Errore: File non trovato: {file_path}")
        return []
    except Exception as e:
        print(f"Errore durante l'estrazione dei dati: {e}")
        return []


def get_tickers(file_path: str = "tickers_data.json") -> List[Dict[str, str]]:
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

if __name__ == "__main__":

    # Specifica il percorso del file di testo
    file_path = "tickers_data.txt"  # Cambia questo percorso con il file che contiene i dati
    data = extract_data_from_file(file_path)

    if data:
        # Stampa i dati in formato JSON
        print(json.dumps(data, indent=4))

        # Salva l'output JSON in un file
        output_file_path = "tickers_data.json"
        try:
            with open(output_file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)
            print(f"Dati salvati correttamente in {output_file_path}")
        except Exception as e:
            print(f"Errore durante il salvataggio del file JSON: {e}")
    else:
        print("Nessun dato trovato.")
