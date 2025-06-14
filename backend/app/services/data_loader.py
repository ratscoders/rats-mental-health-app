import pandas as pd
import os
from typing import Dict, List, Optional, Union
from pathlib import Path

# Caminho base para os dados
# Resolve o diretório "data/raw" na raiz do repositório
DATA_DIR = Path(__file__).resolve().parents[3] / "data" / "raw"

def load_dataset(dataset_id: str) -> pd.DataFrame:
    """
    Carrega um dataset específico
    
    Args:
        dataset_id: ID do dataset a ser carregado
    
    Returns:
        DataFrame com os dados carregados
    
    Raises:
        FileNotFoundError: Se o arquivo não for encontrado
    """
    # Mapeamento de IDs para nomes de arquivos
    # Os arquivos mantêm os nomes originais utilizados no repositório
    dataset_mapping = {
        "mental-illnesses-prevalence": "1- mental-illnesses-prevalence.csv",
        "burden-disease-mental-illness": "2- burden-disease-from-each-mental-illness(1).csv",
        "depression-prevalence-coverage": "3- adult-population-covered-in-primary-data-on-the-prevalence-of-major-depression.csv",
        "mental-illnesses-coverage": "4- adult-population-covered-in-primary-data-on-the-prevalence-of-mental-illnesses.csv",
        "anxiety-treatment-gap": "5- anxiety-disorders-treatment-gap.csv",
        "us-depressive-symptoms": "6- depressive-symptoms-across-us-population.csv",
        "countries-with-data": "7- number-of-countries-with-primary-data-on-prevalence-of-mental-illnesses-in-the-global-burden-of-disease-study.csv",
    }
    
    if dataset_id not in dataset_mapping:
        raise FileNotFoundError(f"Dataset {dataset_id} não encontrado")
    
    file_path = DATA_DIR / dataset_mapping[dataset_id]
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo {file_path} não encontrado")
    
    # Carrega o CSV
    df = pd.read_csv(file_path)
    
    # Processamento específico para cada dataset
    if dataset_id == "mental-illnesses-prevalence":
        df = process_prevalence_data(df)
    elif dataset_id == "burden-disease-mental-illness":
        df = process_burden_data(df)
    elif dataset_id == "anxiety-treatment-gap":
        df = process_treatment_gap_data(df)
    
    return df

def process_prevalence_data(df: pd.DataFrame) -> pd.DataFrame:
    """Processa dados de prevalência para formato padronizado"""
    # Renomeia colunas para formato mais amigável
    column_mapping = {
        col: col.replace("disorderssharepopulation", "").title() 
        for col in df.columns if "disorderssharepopulation" in col
    }
    
    df = df.rename(columns=column_mapping)
    return df

def process_burden_data(df: pd.DataFrame) -> pd.DataFrame:
    """Processa dados de carga de doença para formato padronizado"""
    # Simplifica nomes de colunas
    column_mapping = {
        col: col.replace("DALYsrateSexBothAgeAgestandardizedCause", "").title() + "DALYs"
        for col in df.columns if "DALYsrateSexBothAgeAgestandardizedCause" in col
    }
    
    df = df.rename(columns=column_mapping)
    return df

def process_treatment_gap_data(df: pd.DataFrame) -> pd.DataFrame:
    """Processa dados de lacuna de tratamento para formato padronizado"""
    # Simplifica nomes de colunas
    column_mapping = {
        "Potentiallyadequatetreatmentconditional": "AdequateTreatment",
        "Othertreatmentsconditional": "OtherTreatments",
        "Untreatedconditional": "Untreated"
    }
    
    df = df.rename(columns=column_mapping)
    return df

def filter_by_region(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """Filtra dados por região"""
    if region == "global":
        return df
    
    # Mapeamento de regiões para códigos de países
    region_codes = {
        "americas": ["USA", "CAN", "MEX", "BRA", "ARG", "COL", "PER", "CHL", "VEN", "ECU"],
        "europe": ["GBR", "FRA", "DEU", "ITA", "ESP", "PRT", "NLD", "BEL", "CHE", "AUT"],
        "asia": ["CHN", "JPN", "IND", "IDN", "PAK", "BGD", "PHL", "VNM", "THA", "MYS"],
        "africa": ["ZAF", "NGA", "EGY", "DZA", "MAR", "TUN", "LBY", "SDN", "ETH", "KEN"],
        "oceania": ["AUS", "NZL", "PNG", "FJI", "SLB", "VUT", "WSM", "TON", "KIR", "FSM"]
    }
    
    if region not in region_codes:
        return df
    
    return df[df["Code"].isin(region_codes[region])]

def filter_by_year(df: pd.DataFrame, year: Union[int, str]) -> pd.DataFrame:
    """Filtra dados por ano"""
    year_num = int(year) if isinstance(year, str) else year
    return df[df["Year"] == year_num]

def filter_by_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """Filtra dados por período"""
    if period == "all":
        return df
    
    current_year = 2023  # Ano atual para referência
    
    if period == "recent":
        # Últimos 5 anos
        return df[df["Year"] >= current_year - 5]
    elif period == "decade":
        # Última década
        return df[df["Year"] >= current_year - 10]
    
    return df
