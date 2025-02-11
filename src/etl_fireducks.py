import fireducks.pandas as pd
from tqdm import tqdm
import time

total_linhas = 1_000_000_000  # Total de linhas conhecido
chunksize = 100_000_000       # Tamanho do chunk
filename = "data/measurements.txt"  # Caminho para o arquivo

def process_chunk(chunk):
    # Agrega os dados de cada chunk utilizando FireDucks (que já otimiza internamente)
    aggregated = chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()
    return aggregated

def create_df_with_fireducks(filename, total_linhas, chunksize=chunksize):
    total_chunks = total_linhas // chunksize + (1 if total_linhas % chunksize else 0)
    results = []
    
    # Lê o arquivo em chunks; FireDucks utiliza a mesma API do pandas
    reader = pd.read_csv(filename, sep=';', header=None, names=['station', 'measure'], chunksize=chunksize)
    
    # Processa cada chunk sequencialmente com barra de progresso
    for chunk in tqdm(reader, total=total_chunks, desc="Processando"):
        results.append(process_chunk(chunk))
        
    # Concatena os resultados de todos os chunks
    final_df = pd.concat(results, ignore_index=True)
    
    # Agrega os resultados finais e ordena pelo campo 'station'
    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')
    
    return final_aggregated_df

if __name__ == "__main__":
    print("Iniciando o processamento do arquivo.")
    start_time = time.time()
    df = create_df_with_fireducks(filename, total_linhas, chunksize)
    took = time.time() - start_time

    print(df.head())
    print(f"Processing took: {took:.2f} sec")
