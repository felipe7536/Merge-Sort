import csv
import os
import heapq
import tempfile
import random
import string

# --- Funções Auxiliares de Conversão e Comparação ---

def _tentar_converter_chave(chave_str):
    """Tenta converter uma chave string para float. Se não for possível, retorna a string original."""
    try:
        return float(chave_str)
    except (ValueError, TypeError):
        return chave_str

class HeapItem:
    """
    Um objeto wrapper para ser usado no heap.
    Isso é necessário para controlar a lógica de comparação para ordenação
    crescente e decrescente de forma robusta, sem truques que falham com números.
    """
    def __init__(self, item_tupla, crescente=True):
        self.item = item_tupla  # (chave_convertida, linha_original, indice_do_arquivo_run)
        self.crescente = crescente

    def __lt__(self, other):
        # O heapq é um min-heap (sempre coloca o menor item no topo).
        # Para ordem CRESCENTE, queremos que o menor item (a < b) venha primeiro.
        # Para ordem DECRESCENTE, queremos o oposto, que o maior item (a > b) venha primeiro.
        # Invertendo a lógica de "menor que" (<), efetivamente simulamos um max-heap.
        if self.crescente:
            return self.item[0] < other.item[0]
        else:
            return self.item[0] > other.item[0]

# --- Funções Principais da Ordenação Externa ---

def ordenar_run_em_memoria(run, chave_idx, crescente=True):
    """
    Ordena uma lista de registros (run) em memória.
    A chave de ordenação é convertida para número, se possível.
    """
    # A função `key` extrai a coluna, tenta convertê-la para número e retorna o valor para ordenação
    run.sort(key=lambda linha: _tentar_converter_chave(linha[chave_idx]), reverse=not crescente)
    return run

def dividir_em_runs(arquivo_csv, chave_idx, tamanho_max=1000, crescente=True):
    """
    Divide o arquivo CSV em pequenos blocos (runs), cada um cabendo na memória.
    Cada run é ordenado e salvo em um arquivo temporário.
    """
    runs_temporarios = []
    try:
        with open(arquivo_csv, mode='r', newline='', encoding='utf-8') as csvfile:
            leitor = csv.reader(csvfile)
            cabecalho = next(leitor)  # Lê e guarda o cabeçalho

            buffer = []
            for linha in leitor:
                buffer.append(linha)
                if len(buffer) >= tamanho_max:
                    run_ordenado = ordenar_run_em_memoria(buffer, chave_idx, crescente)
                    
                    # Cria um arquivo temporário para o run ordenado
                    run_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8', suffix='.csv')
                    escritor = csv.writer(run_file)
                    # Não precisamos do cabeçalho em cada run, ele será escrito uma vez no arquivo final
                    escritor.writerows(run_ordenado)
                    run_file.close()
                    runs_temporarios.append(run_file.name)
                    buffer = [] # Limpa o buffer para o próximo run

            # Processa o restante do buffer que não completou um run de tamanho_max
            if buffer:
                run_ordenado = ordenar_run_em_memoria(buffer, chave_idx, crescente)
                run_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', encoding='utf-8', suffix='.csv')
                escritor = csv.writer(run_file)
                escritor.writerows(run_ordenado)
                run_file.close()
                runs_temporarios.append(run_file.name)
            
            return runs_temporarios, cabecalho

    except FileNotFoundError:
        print(f"Erro: O arquivo '{arquivo_csv}' não foi encontrado.")
        return [], None
    except Exception as e:
        print(f"Ocorreu um erro ao dividir o arquivo em runs: {e}")
        # Limpa arquivos temporários em caso de erro
        for run_path in runs_temporarios:
            os.remove(run_path)
        return [], None


def merge_runs(runs, chave_idx, cabecalho, arquivo_saida_path, crescente=True):
    """
    Faz o merge dos runs ordenados em um único arquivo final.
    Utiliza um min-heap para mesclar todos os k runs de forma eficiente (k-way merge).
    """
    arquivos_abertos = [open(run, mode='r', newline='', encoding='utf-8') for run in runs]
    leitores = [csv.reader(arquivo) for arquivo in arquivos_abertos]
    
    heap = []

    # Carrega a primeira linha de cada run no heap
    for i, leitor in enumerate(leitores):
        try:
            primeira_linha = next(leitor)
            chave = _tentar_converter_chave(primeira_linha[chave_idx])
            # A tupla contém: (chave_para_ordenar, linha_completa, indice_do_leitor)
            heap_item = HeapItem((chave, primeira_linha, i), crescente)
            heapq.heappush(heap, heap_item)
        except StopIteration:
            # Arquivo de run está vazio, o que não deveria acontecer se ele foi criado
            continue

    # Abre o arquivo de saída final para escrita
    with open(arquivo_saida_path, mode='w', newline='', encoding='utf-8') as arquivo_saida:
        escritor = csv.writer(arquivo_saida)
        escritor.writerow(cabecalho) # Escreve o cabeçalho

        # Enquanto o heap não estiver vazio, significa que ainda há linhas para processar
        while heap:
            # 1. Pega o menor item do heap (graças à classe HeapItem)
            menor_item = heapq.heappop(heap)
            _, menor_linha, origem_idx = menor_item.item

            # 2. Escreve a linha no arquivo de saída
            escritor.writerow(menor_linha)

            # 3. Pega a próxima linha do mesmo arquivo de onde veio o menor item
            try:
                proxima_linha = next(leitores[origem_idx])
                chave = _tentar_converter_chave(proxima_linha[chave_idx])
                
                # 4. Adiciona a nova linha ao heap
                novo_item = HeapItem((chave, proxima_linha, origem_idx), crescente)
                heapq.heappush(heap, novo_item)
            except StopIteration:
                # Chegou ao fim de um dos arquivos de run, então não adiciona mais nada dele ao heap
                continue

    # Fecha todos os arquivos de run abertos
    for arquivo in arquivos_abertos:
        arquivo.close()

    # Remove os arquivos temporários (runs)
    for run_path in runs:
        os.remove(run_path)


def ordenacao_externa_csv(arquivo_csv, arquivo_saida, chave, crescente=True, tamanho_max_memoria=1000):
    """
    Função principal para ordenar externamente um arquivo CSV.

    Parâmetros:
    - arquivo_csv (str): Caminho do arquivo CSV de entrada.
    - arquivo_saida (str): Caminho do arquivo CSV de saída ordenado.
    - chave (str ou int): Nome ou índice da coluna a ser usada como chave de ordenação.
    - crescente (bool): True para ordem crescente (padrão), False para decrescente.
    - tamanho_max_memoria (int): Número máximo de linhas a serem mantidas em memória por vez.
    """
    print("Iniciando ordenação externa...")

    # Determina o índice da coluna chave
    try:
        with open(arquivo_csv, mode='r', newline='', encoding='utf-8') as f:
            leitor = csv.reader(f)
            cabecalho_temp = next(leitor)
            if isinstance(chave, int):
                if not 0 <= chave < len(cabecalho_temp):
                    raise IndexError(f"Índice da chave '{chave}' fora do intervalo.")
                chave_idx = chave
            else:
                chave_idx = cabecalho_temp.index(chave)
    except FileNotFoundError:
        print(f"Erro: Arquivo de entrada '{arquivo_csv}' não encontrado.")
        return
    except (ValueError, IndexError) as e:
        print(f"Erro com a chave de ordenação: {e}")
        return

    # Fase 1: Dividir o arquivo grande em 'runs' menores e ordenados
    print(f"1. Dividindo '{arquivo_csv}' em runs ordenados de até {tamanho_max_memoria} linhas...")
    runs, cabecalho = dividir_em_runs(arquivo_csv, chave_idx, tamanho_max_memoria, crescente)
    if not runs:
        print("Nenhum run foi gerado. Abortando.")
        return
    print(f"   -> {len(runs)} runs temporários criados.")

    # Fase 2: Fazer o merge dos runs em um único arquivo de saída
    print(f"2. Mesclando os {len(runs)} runs em '{arquivo_saida}'...")
    merge_runs(runs, chave_idx, cabecalho, arquivo_saida, crescente)

    print("-" * 30)
    print(f"SUCESSO! Arquivo final ordenado salvo em: {arquivo_saida}")
    print("-" * 30)

# --- Bloco de Exemplo de Uso e Teste ---
if __name__ == "__main__":

    def gerar_arquivo_csv_teste(nome_arquivo, num_linhas):
        """Função para criar um arquivo CSV grande para testes."""
        print(f"Gerando arquivo de teste '{nome_arquivo}' com {num_linhas} linhas...")
        cabecalho = ['id', 'nome', 'cpf', 'valor_aleatorio']
        with open(nome_arquivo, 'w', newline='', encoding='utf-8') as f:
            escritor = csv.writer(f)
            escritor.writerow(cabecalho)
            for i in range(num_linhas):
                nome = ''.join(random.choices(string.ascii_lowercase, k=10))
                cpf = f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"
                valor = round(random.uniform(1.0, 10000.0), 2)
                escritor.writerow([i, nome, cpf, valor])
        print("Arquivo de teste gerado.")

    # --- Configurações do Teste ---
    ARQUIVO_ENTRADA = "grande_arquivo.csv"
    ARQUIVO_SAIDA_CRESCENTE = "arquivo_ordenado_crescente.csv"
    ARQUIVO_SAIDA_DECRESCENTE = "arquivo_ordenado_decrescente.csv"
    NUM_LINHAS = 20000  # Um número grande o suficiente para gerar vários runs
    TAMANHO_BUFFER = 5000  # Simula uma memória que só aguenta 5000 linhas por vez

    # 1. Gerar o arquivo de teste
    gerar_arquivo_csv_teste(ARQUIVO_ENTRADA, NUM_LINHAS)
    
    print("\n" + "="*50)
    print("TESTE 1: ORDENAÇÃO CRESCENTE POR 'valor_aleatorio'")
    print("="*50)
    # 2. Executar a ordenação externa crescente pela coluna 'valor_aleatorio'
    ordenacao_externa_csv(
        arquivo_csv=ARQUIVO_ENTRADA,
        arquivo_saida=ARQUIVO_SAIDA_CRESCENTE,
        chave='valor_aleatorio',  # Pode ser o nome da coluna
        crescente=True,
        tamanho_max_memoria=TAMANHO_BUFFER
    )

    print("\n" + "="*50)
    print("TESTE 2: ORDENAÇÃO DECRESCENTE POR 'id'")
    print("="*50)
    # 3. Executar a ordenação externa decrescente pela coluna de índice 0 ('id')
    ordenacao_externa_csv(
        arquivo_csv=ARQUIVO_ENTRADA,
        arquivo_saida=ARQUIVO_SAIDA_DECRESCENTE,
        chave=0,  # Pode ser o índice da coluna
        crescente=False,
        tamanho_max_memoria=TAMANHO_BUFFER
    )
    
    # 4. Limpeza dos arquivos gerados
    print("\nLimpando arquivos gerados...")
    os.remove(ARQUIVO_ENTRADA)
    os.remove(ARQUIVO_SAIDA_CRESCENTE)
    os.remove(ARQUIVO_SAIDA_DECRESCENTE)
    print("Limpeza concluída.")
