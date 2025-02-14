# Um Bilhão de Linhas: Desafio de Processamento de Dados com Python

## Diagrama

![arquitetura](/img/arquitetura_1.1.png)

## Introdução

O objetivo deste projeto é demonstrar como processar eficientemente um arquivo de dados massivo contendo 1 bilhão de linhas (~15GB), especificamente para calcular estatísticas (Incluindo agregação e ordenação que são operações pesadas) utilizando Python. 

Este desafio foi inspirado no [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc), originalmente proposto para Java.

O arquivo de dados consiste em medições de temperatura de várias estações meteorológicas. Cada registro segue o formato `<string: nome da estação>;<double: medição>`, com a temperatura sendo apresentada com precisão de uma casa decimal.

Aqui estão dez linhas de exemplo do arquivo:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. Johns;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

O desafio é desenvolver um programa Python capaz de ler esse arquivo e calcular a temperatura mínima, média (arredondada para uma casa decimal) e máxima para cada estação, exibindo os resultados em uma tabela ordenada por nome da estação.

| station      | min_temperature | mean_temperature | max_temperature |
|--------------|-----------------|------------------|-----------------|
| Abha         | -31.1           | 18.0             | 66.5            |
| Abidjan      | -25.9           | 26.0             | 74.6            |
| Abéché       | -19.8           | 29.4             | 79.9            |
| Accra        | -24.8           | 26.4             | 76.3            |
| Addis Ababa  | -31.8           | 16.0             | 63.9            |
| Adelaide     | -31.8           | 17.3             | 71.5            |
| Aden         | -19.6           | 29.1             | 78.3            |
| Ahvaz        | -24.0           | 25.4             | 72.6            |
| Albuquerque  | -35.0           | 14.0             | 61.9            |
| Alexandra    | -40.1           | 11.0             | 67.9            |
| ...          | ...             | ...              | ...             |
| Yangon       | -23.6           | 27.5             | 77.3            |
| Yaoundé      | -26.2           | 23.8             | 73.4            |
| Yellowknife  | -53.4           | -4.3             | 46.7            |
| Yerevan      | -38.6           | 12.4             | 62.8            |
| Yinchuan     | -45.2           | 9.0              | 56.9            |
| Zagreb       | -39.2           | 10.7             | 58.1            |
| Zanzibar City| -26.5           | 26.0             | 75.2            |
| Zürich       | -42.0           | 9.3              | 63.6            |
| Ürümqi       | -42.1           | 7.4              | 56.7            |
| İzmir        | -34.4           | 17.9             | 67.9            |

## Dependências

Para executar os scripts deste projeto, você precisará das seguintes bibliotecas:

* pandas: `^2.2.2`
* duckdb: `^1.0.0`
* dask: `^2024.7.1`
* polars: `^1.3.0`
* fireducks: `^1.2.1`
* streamlit: `^1.37.0`
* Apache Airflow (via Astro CLI)

## Resultados

Os testes foram realizados em um Laptop com Ubuntu 24.04.2 LTS (Lenovo ThinkPad E14 Gen 3) equipado com um processador AMD Ryzen™ 7 5700U with Radeon™ Graphics × 16 e 16GB de RAM. As implementações utilizaram abordagens puramente Python, Pandas, Dask, Polars, Fireducks e DuckDB. Os resultados de tempo de execução para processar o arquivo de 1 bilhão de linhas são apresentados abaixo:

| Implementação | Tempo |
| --- | --- |
| Bash + awk | 25 minutos |
| Python | 20 minutos |
| Python + Pandas | 358.78 seg |
| Python + Fireducks | 324.40 seg |
| Python + Dask | 242.49 seg  |
| Python + Duckdb | 67.78 seg |
| Python + Polars | 36.57 seg |

![arquitetura](/img/podium_1.png)

Obrigado por [Koen Vossen](https://github.com/koenvo) pela implementação em Polars e [Arthur Julião](https://github.com/ArthurJ) pela implementação em Python e Bash 

## Conclusão

Este desafio destacou claramente a eficácia de diversas bibliotecas Python na manipulação de grandes volumes de dados. Métodos tradicionais como Bash, Python puro e até mesmo o Pandas/Fireducks demandaram uma série de táticas para implementar o processamento em "lotes", enquanto bibliotecas como Dask, Polars e DuckDB provaram ser excepcionalmente eficazes, requerendo menos linhas de código devido à sua capacidade inerente de distribuir os dados em "lotes em streaming" de maneira mais eficiente. 

🏆 O Polars alcançou o menor tempo de execução (36.57 segundos), talvez devido a uma combinação de características arquiteturais e estratégias de processamento superiores:
- Lazy Execution: Permite fusão de múltiplas operações em um único plano otimizado.
- Multi-threading: Aproveita todos os núcleos de CPU disponíveis de forma eficiente.
- Arquitetura em Rust: O código base em Rust oferece performance nativa e gerenciamento eficiente de memória. Alta performance, memória otimizada e paralelismo nativo.

![arquitetura](/img/estrategia_polars.png)

![arquitetura](/img/vantagens_polars.png)

O DuckDB alcançou o segundo menor tempo de execução (67.78 segundos), talvez graças à sua estratégia de execução e processamento de dados, que inclui:
1. Query Execution Model:
   - Processamento vetorizado que quebra operações em blocos otimizados
   - Implementação de um otimizador SQL que reduz operações desnecessárias
   - Compilação interna de queries para acelerar o processamento

2. Processamento de Dados Incremental:
   - Suporte a arquivos Parquet com leitura seletiva de colunas
   - Streaming eficiente de dados, processando blocos de linhas em formato vetorizado
   - Cache inteligente que reutiliza resultados para evitar reprocessamento

3. Multi-threading Eficiente:
   - Aproveitamento automático de múltiplos núcleos da CPU
   - Paralelização inteligente de queries em grandes arquivos
   - Redução significativa do tempo de resposta através de execução paralela

![arquitetura](/img/estrategias_processamento_duckdb.png)

![arquitetura](/img/vantagens_duckdb.png)

Esses resultados reforçam a importância de escolher a ferramenta adequada para análise de dados em larga escala. Polars e DuckDB se destacaram por combinar execução colunar, gerenciamento eficiente de memória e paralelização automática. O Polars, em particular, demonstrou como uma implementação em Rust com lazy evaluation pode superar significativamente abordagens tradicionais. Isso evidencia que Python, munido das bibliotecas certas, continua sendo uma opção poderosa para big data.

## Como Executar

Para executar este projeto e reproduzir os resultados:

1. Clone esse repositório
2. Definir a versão do Python usando o `pyenv local 3.12.3`
3. Execute os comandos:
   ```bash
   poetry env use 3.12.3
   poetry install --no-root
   poetry lock --no-update
   ```
4. Execute o comando `python src/create_measurements.py` para gerar o arquivo de teste (caso queira aumentar ou diminuir o tamanho do arquivo, altere o valor da variavel num_rows_to_create=1_000_000_000)
5. Aguarde alguns minutos para a geração completa do arquivo
6. Execute os scripts:
   ```bash
   python src/etl_python.py
   python src/etl_pandas.py
   python src/etl_fireducks.py
   python src/etl_dask.py
   python src/etl_polars.py
   python src/etl_duckdb.py
   ```

## Executando o Apache Airflow

Este projeto utiliza o Apache Airflow para gerenciar e automatizar os fluxos de processamento de dados. Para iniciar o Airflow, siga os passos abaixo:

1. Navegue até o diretório do Airflow:
   ```bash
   cd src/airflow
   ```
2. Inicie o ambiente com Astro CLI:
   ```bash
   astro dev start
   ```
3. Acesse a interface web do Airflow em `http://localhost:8080`
4. Para verificar os DAGs disponíveis e iniciar a execução, ative os DAGs necessários na interface

Caso queira parar a execução do Airflow, utilize:

```bash
astro dev stop
```

## Bonus

Para rodar o script Bash descrito, você precisa seguir alguns passos simples. Primeiro, assegure-se de que você tenha um ambiente Unix-like, como Linux ou macOS, que suporta scripts Bash nativamente. Além disso, verifique se as ferramentas utilizadas no script (`wc`, `head`, `pv`, `awk`, e `sort`) estão instaladas em seu sistema. A maioria dessas ferramentas vem pré-instalada em sistemas Unix-like, mas `pv` (Pipe Viewer) pode precisar ser instalado manualmente.

### Instalando o Pipe Viewer (pv)

Se você não tem o `pv` instalado, pode facilmente instalá-lo usando o gerenciador de pacotes do seu sistema. Por exemplo:

* No Ubuntu/Debian:
    
    ```bash
    sudo apt-get update
    sudo apt-get install pv
    ```
    
* No macOS (usando [Homebrew](https://brew.sh/)):
    
    ```bash
    brew install pv
    ```
    
### Preparando o Script

1. Dê permissão de execução para o arquivo script. Abra um terminal e execute:
    
    ```bash
    chmod +x process_measurements.sh
    ```

2. Rode o script. Abra um terminal e execute:
   
   ```bash
   ./src/using_bash_and_awk.sh 1000
   ```

Neste exemplo, apenas as primeiras 1000 linhas serão processadas.

Ao executar o script, você verá a barra de progresso (se pv estiver instalado corretamente) e, eventualmente, a saída esperada no terminal ou em um arquivo de saída, se você decidir modificar o script para direcionar a saída.

## Contato

Para dúvidas, sugestões ou feedbacks:

* **Thiago Silva** - [Linkedin](https://www.linkedin.com/in/thiagosilvafarias/)




