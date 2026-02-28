"""
Validação: Comparar dados de formulários entre páginas "Visão clientes e ECs" e "Visão auditoria".

Objetivo: Confirmar que DISTINCTCOUNT corrige a sobre-contagem de formulários causada
pela multiplicação many-to-many entre FactAprovacaoPrecoParceiro e RespostasFormulario.

Conexão: Databricks via U2M OAuth (databricks-cli profile).
Dados do formulário: SharePoint Excel (informar caminho local se disponível).

Uso:
  python validar_formularios.py
  python validar_formularios.py --excel "C:/caminho/para/Projeto Preço Parceiro.xlsx"
"""

import os
import sys
import argparse
from datetime import datetime

# ============================================================
# CONFIG DATABRICKS
# ============================================================
HOST = "adb-7941093640821140.0.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/ce56ec5f5d0a3e07"
PROFILE = "adb-7941093640821140"


def get_databricks_connection():
    """Conecta ao SQL Warehouse via U2M OAuth (databricks-cli profile)."""
    from databricks import sql
    print("🔗 Conectando ao Databricks via CLI profile...")
    conn = sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        auth_type="databricks-cli",
        profile=PROFILE,
    )
    print("   ✅ Conectado!")
    return conn


# ============================================================
# QUERY: Mesma query da FactAprovacaoPrecoParceiro do Power BI
# (simplificada para extrair NumeroOS + NomeCliente + NomeEC)
# ============================================================
QUERY_FACT = """
WITH param_logs AS (
  SELECT
    dmplv.ClientId AS CodigoCliente,
    CAST(dmplv.ParameterLogOrgValueModificationTimestamp AS TIMESTAMP) AS ts,
    COALESCE(dmplv.OldValueDescription, '') AS old_values,
    COALESCE(dmplv.NewValueDescription, '') AS new_values
  FROM hive_metastore.gold.Dim_MaintenanceParameterLogValue AS dmplv
  WHERE dmplv.ParameterId = 586
),
sets AS (
  SELECT
    CodigoCliente,
    ts,
    array_distinct(
      filter(
        split(regexp_replace(old_values, '\\\\s+', ''), '[;,]+'),
        x -> x <> ''
      )
    ) AS old_set,
    array_distinct(
      filter(
        split(regexp_replace(new_values, '\\\\s+', ''), '[;,]+'),
        x -> x <> ''
      )
    ) AS new_set
  FROM param_logs
),
adds AS (
  SELECT CodigoCliente, ts, CAST(approver AS STRING) AS approver
  FROM sets LATERAL VIEW explode(new_set) ns AS approver
  WHERE NOT array_contains(old_set, approver)
),
removes AS (
  SELECT CodigoCliente, ts, CAST(approver AS STRING) AS approver
  FROM sets LATERAL VIEW explode(old_set) os AS approver
  WHERE NOT array_contains(new_set, approver)
),
events AS (
  SELECT CodigoCliente, approver, ts, 'ADD' AS event_type FROM adds
  UNION ALL
  SELECT CodigoCliente, approver, ts, 'REMOVE' AS event_type FROM removes
),
ordered_events AS (
  SELECT
    CodigoCliente, approver, event_type, ts,
    LEAD(ts) OVER (PARTITION BY CodigoCliente, approver ORDER BY ts) AS DataFim
  FROM events
),
param_intervals AS (
  SELECT CodigoCliente, approver AS Aprovador, ts AS DataInicio, DataFim
  FROM ordered_events WHERE event_type = 'ADD'
),
tabela_preco_parceiro AS (
  SELECT DISTINCT OrderServiceId
  FROM gold.dim_maintenancelogpriceregulatorpartner
  WHERE SendDate IS NOT NULL
)

SELECT
    fmi.MaintenanceId AS NumeroOS,
    dfc.CustomerShortName AS NomeCliente,
    dmm.MerchantShortenedName AS NomeEC,
    dmm.StateName AS UFEC

FROM hive_metastore.gold.fact_maintenanceitems AS fmi

  LEFT JOIN hive_metastore.gold.fact_maintenanceservices AS fms
    ON fmi.MaintenanceId = fms.OrderServiceCode

  LEFT JOIN hive_metastore.gold.dim_maintenancetypes AS fmt
    ON fms.Sk_MaintenanceType = fmt.Sk_MaintenanceType

  LEFT JOIN hive_metastore.gold.dim_maintenancemerchants AS dmm
    ON fms.Sk_MaintenanceMerchant = dmm.Sk_MaintenanceMerchant

  LEFT JOIN hive_metastore.gold.dim_maintenancevehicles AS dmv
    ON fms.Sk_MaintenanceVehicle = dmv.Sk_MaintenanceVehicle

  LEFT JOIN hive_metastore.gold.dim_fuelcustomers AS dfc
    ON fms.Sk_FuelCustomer = dfc.Sk_FuelCustomer

  LEFT JOIN hive_metastore.gold.dim_webusers AS dwu
    ON fms.FirstApproverCode = dwu.WebUserSourceCode

  LEFT JOIN gold.dim_maintenancelogpriceregulatorpartner AS dmlprp
    ON fmi.MaintenanceItemSourceCode = dmlprp.OrderServiceItemId

  LEFT JOIN hive_metastore.gold.dim_maintenanceitemmanufacturers AS dmif
    ON fmi.Sk_ServiceItemManufacturer = dmif.Sk_ServiceItemManufacturer

  LEFT JOIN tabela_preco_parceiro
    ON fmi.MaintenanceId = tabela_preco_parceiro.OrderServiceId

  LEFT JOIN hive_metastore.gold.dim_maintenancelabors AS dml
    ON fmi.Sk_MaintenanceLabor = dml.Sk_MaintenanceLabor

WHERE 1=1
  AND fmi.CancellationTimestamp IS NULL
  AND fmi.ItemDisapprovalTimestamp IS NULL
  AND fms.FirstApprovalTimestamp >= TIMESTAMP '2025-04-01'
  AND (dml.LaborName IS NULL OR dml.LaborName NOT LIKE '%GUINCHO%')
  AND (
    tabela_preco_parceiro.OrderServiceId IS NOT NULL
    OR
    EXISTS (
      SELECT 1
      FROM param_intervals pi
      WHERE pi.CodigoCliente = dmv.CustomerId
        AND pi.Aprovador = CAST(dwu.WebUserSourceCode AS STRING)
        AND fms.FirstApprovalTimestamp >= pi.DataInicio
        AND (pi.DataFim IS NULL OR fms.FirstApprovalTimestamp < pi.DataFim)
    )
  )
"""


def load_fact_data(conn):
    """Carrega os dados da FactAprovacaoPrecoParceiro do Databricks."""
    import pandas as pd
    print("\n📊 Carregando FactAprovacaoPrecoParceiro do Databricks...")
    print("   (isso pode levar alguns minutos)")
    cursor = conn.cursor()
    cursor.execute(QUERY_FACT)
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=cols)
    cursor.close()
    print(f"   ✅ {len(df):,} linhas carregadas")
    return df


def load_formulario(excel_path=None):
    """Carrega os dados do formulário (RespostasFormulario)."""
    import pandas as pd

    if excel_path and os.path.exists(excel_path):
        print(f"\n📋 Carregando formulário de: {excel_path}")

        # Listar abas disponíveis
        xl = pd.ExcelFile(excel_path)
        print(f"   Abas disponíveis: {xl.sheet_names}")

        # Tentar nomes comuns
        target_sheets = ["TabelaPrecoParceiro", "Respostas", "Form1", "Sheet1", "Planilha1"]
        df = None

        for sheet_name in target_sheets:
            if sheet_name in xl.sheet_names:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                print(f"   Usando aba: '{sheet_name}'")
                break

        if df is None:
            # Se nenhuma aba conhecida, usa a primeira
            first_sheet = xl.sheet_names[0]
            df = pd.read_excel(excel_path, sheet_name=first_sheet)
            print(f"   Usando primeira aba: '{first_sheet}'")

        # Encontrar a coluna de número da ordem
        col_ordem = None
        for col in df.columns:
            if "mero da ordem" in str(col).lower() or "numero" in str(col).lower().replace("ú", "u"):
                col_ordem = col
                break
            if "ordem" in str(col).lower() and "n" in str(col).lower():
                col_ordem = col
                break

        if col_ordem is None:
            print(f"   Colunas encontradas: {list(df.columns)}")
            # Tentar a coluna que mais parece ser um número de OS
            for col in df.columns:
                if "ordem" in str(col).lower():
                    col_ordem = col
                    break

        if col_ordem is None:
            print("   ❌ Não consegui identificar a coluna de número da ordem.")
            print(f"   Colunas: {list(df.columns)}")
            return None

        print(f"   Coluna de OS: '{col_ordem}'")
        df = df.rename(columns={col_ordem: "Número da ordem"})

        # Encontrar a coluna de aceitação
        col_aceite = None
        for col in df.columns:
            if "aceitou" in str(col).lower() or "aceita" in str(col).lower():
                col_aceite = col
                break

        if col_aceite:
            df = df.rename(columns={col_aceite: "EC aceitou a negociação?"})
        else:
            print("   ⚠️ Coluna 'EC aceitou a negociação?' não encontrada.")
            print(f"   Colunas: {list(df.columns)}")

        df["Número da ordem"] = df["Número da ordem"].astype(str).str.strip()
        df = df[df["Número da ordem"].notna() & (df["Número da ordem"] != "") & (df["Número da ordem"] != "nan")]
        print(f"   ✅ {len(df):,} respostas carregadas")
        return df
    else:
        print("\n⚠️  Arquivo Excel do formulário não encontrado.")
        print("   Procurando localmente...")
        # Tentar encontrar o arquivo
        possible_paths = [
            os.path.expanduser("~/Downloads/Projeto Preço Parceiro.xlsx"),
            os.path.expanduser("~/Documents/Projeto Preço Parceiro.xlsx"),
            os.path.expanduser("~/OneDrive - EDENRED/Documents/Projeto Preço Parceiro.xlsx"),
        ]
        for p in possible_paths:
            if os.path.exists(p):
                print(f"   Encontrado: {p}")
                return load_formulario(p)

        print("   ❌ Não encontrado. Use --excel para informar o caminho.")
        print("   O script vai continuar apenas com os dados do Databricks.\n")
        return None


def run_validation(df_fact, df_form):
    """Executa a validação comparativa COUNT vs DISTINCTCOUNT."""
    import pandas as pd

    print("\n" + "=" * 80)
    print("  VALIDAÇÃO: COUNT vs DISTINCTCOUNT — Total de respostas formulário")
    print("=" * 80)

    if df_form is None:
        print("\n⚠️  Sem dados do formulário — análise parcial (apenas duplicatas na Fact).\n")
        # Mesmo sem formulário, podemos mostrar quantas OS são duplicadas na Fact
        os_counts = df_fact.groupby("NumeroOS").size().reset_index(name="itens_por_os")
        duplicadas = os_counts[os_counts["itens_por_os"] > 1]
        total_os = len(os_counts)
        total_linhas = len(df_fact)

        print(f"  📊 Total de linhas na FactAprovacaoPrecoParceiro: {total_linhas:,}")
        print(f"  📊 Total de OS distintas: {total_os:,}")
        print(f"  📊 Fator de multiplicação médio: {total_linhas / total_os:.2f}x")
        print(f"  📊 OS com múltiplos itens (peças): {len(duplicadas):,} ({100*len(duplicadas)/total_os:.1f}%)")
        print(f"\n  ⚡ Isso significa que COUNT() inflaria as contagens em ~{total_linhas / total_os:.2f}x")
        print(f"     enquanto DISTINCTCOUNT() retornaria o valor correto.\n")

        # Por cliente
        print("  📋 Top 10 clientes por fator de multiplicação:")
        print("  " + "-" * 76)
        cliente_stats = df_fact.groupby("NomeCliente").agg(
            total_linhas=("NumeroOS", "count"),
            os_distintas=("NumeroOS", "nunique"),
        ).reset_index()
        cliente_stats["fator"] = cliente_stats["total_linhas"] / cliente_stats["os_distintas"]
        cliente_stats = cliente_stats.sort_values("fator", ascending=False).head(10)

        print(f"  {'Cliente':<35} {'Linhas':>8} {'OS Dist':>8} {'Fator':>8}")
        print("  " + "-" * 76)
        for _, row in cliente_stats.iterrows():
            nome = str(row["NomeCliente"])[:34]
            print(f"  {nome:<35} {int(row['total_linhas']):>8,} {int(row['os_distintas']):>8,} {row['fator']:>7.2f}x")

        return

    # ========================================
    # COM FORMULÁRIO: Comparação completa
    # ========================================

    # Converter NumeroOS para string para fazer o join
    df_fact["NumeroOS_str"] = df_fact["NumeroOS"].astype(str).str.strip()
    df_form["NumeroOS_str"] = df_form["Número da ordem"].astype(str).str.strip()

    # OS que têm formulário preenchido
    os_com_formulario = set(df_form["NumeroOS_str"].unique())
    os_na_fact = set(df_fact["NumeroOS_str"].unique())
    os_match = os_com_formulario & os_na_fact

    print(f"\n  📊 Resumo geral:")
    print(f"     Respostas no formulário (linhas): {len(df_form):,}")
    print(f"     OS distintas no formulário: {len(os_com_formulario):,}")
    print(f"     OS distintas na Fact: {len(os_na_fact):,}")
    print(f"     OS em comum (match): {len(os_match):,}")

    # ========================================
    # Simulação: COUNT vs DISTINCTCOUNT por Cliente
    # ========================================
    print(f"\n  📋 Comparação COUNT vs DISTINCTCOUNT por Cliente:")
    print("  " + "=" * 100)

    # Fazer o join many-to-many (simula o comportamento do Power BI)
    # Cada linha da Fact se junta com cada resposta do formulário que tem o mesmo NumeroOS
    df_fact_form = df_fact.merge(
        df_form[["NumeroOS_str", "EC aceitou a negociação?"]],
        on="NumeroOS_str",
        how="inner"
    )

    # Agrupamento por cliente — simulando o visual da tabela
    by_cliente = df_fact.groupby("NomeCliente").agg(
        os_distintas=("NumeroOS_str", "nunique"),
    ).reset_index()

    # COUNT por cliente (o que o Power BI fazia ANTES da correção)
    # Na realidade, o COUNT era sobre RespostasFormulario[Número da ordem]
    # mas filtrado via o relacionamento com a Fact.
    # O resultado é o número de linhas no join many-to-many.
    count_por_cliente = df_fact_form.groupby("NomeCliente").agg(
        count_formularios=("NumeroOS_str", "count"),
    ).reset_index()

    # DISTINCTCOUNT por cliente (o que o Power BI faz DEPOIS da correção)
    distinctcount_por_cliente = df_fact_form.groupby("NomeCliente").agg(
        distinctcount_formularios=("NumeroOS_str", "nunique"),
    ).reset_index()

    # Recusas com COUNT
    df_recusas = df_fact_form[df_fact_form["EC aceitou a negociação?"] == "Não"]
    count_recusas = df_recusas.groupby("NomeCliente").agg(
        count_recusas=("NumeroOS_str", "count"),
    ).reset_index()
    distinctcount_recusas = df_recusas.groupby("NomeCliente").agg(
        distinctcount_recusas=("NumeroOS_str", "nunique"),
    ).reset_index()

    # Merge tudo
    resultado = by_cliente.merge(count_por_cliente, on="NomeCliente", how="left")
    resultado = resultado.merge(distinctcount_por_cliente, on="NomeCliente", how="left")
    resultado = resultado.merge(count_recusas, on="NomeCliente", how="left")
    resultado = resultado.merge(distinctcount_recusas, on="NomeCliente", how="left")
    resultado = resultado.fillna(0)

    # Calcular diferença
    resultado["diff_formularios"] = resultado["count_formularios"] - resultado["distinctcount_formularios"]
    resultado["diff_recusas"] = resultado["count_recusas"] - resultado["distinctcount_recusas"]

    # Filtrar apenas clientes com formulários
    resultado = resultado[resultado["count_formularios"] > 0].sort_values("diff_formularios", ascending=False)

    print(f"\n  {'Cliente':<30} {'COUNT':>8} {'DISTINCT':>9} {'DIFF':>6} {'COUNT_R':>8} {'DIST_R':>7} {'DIFF_R':>7}")
    print("  " + "-" * 100)

    for _, row in resultado.iterrows():
        nome = str(row["NomeCliente"])[:29]
        diff_f = int(row["diff_formularios"])
        diff_r = int(row["diff_recusas"])
        flag_f = " ⚠️" if diff_f > 0 else " ✅"
        print(
            f"  {nome:<30} "
            f"{int(row['count_formularios']):>8,} "
            f"{int(row['distinctcount_formularios']):>9,} "
            f"{diff_f:>+6} "
            f"{int(row['count_recusas']):>8,} "
            f"{int(row['distinctcount_recusas']):>7,} "
            f"{diff_r:>+7}"
            f"{flag_f}"
        )

    # Totais
    total_count = int(resultado["count_formularios"].sum())
    total_distinct = int(resultado["distinctcount_formularios"].sum())
    total_count_r = int(resultado["count_recusas"].sum())
    total_distinct_r = int(resultado["distinctcount_recusas"].sum())

    print("  " + "-" * 100)
    print(
        f"  {'TOTAL':<30} "
        f"{total_count:>8,} "
        f"{total_distinct:>9,} "
        f"{total_count - total_distinct:>+6} "
        f"{total_count_r:>8,} "
        f"{total_distinct_r:>7,} "
        f"{total_count_r - total_distinct_r:>+7}"
    )

    # ========================================
    # Resumo
    # ========================================
    print(f"\n  📊 RESULTADO FINAL:")
    print(f"     Total formulários (COUNT — ERRADO):        {total_count:>6,}")
    print(f"     Total formulários (DISTINCTCOUNT — CORRETO): {total_distinct:>6,}")
    print(f"     Sobre-contagem:                             {total_count - total_distinct:>+6,} ({100*(total_count - total_distinct)/total_distinct:.1f}% a mais)")
    print(f"")
    print(f"     Total recusas (COUNT — ERRADO):        {total_count_r:>6,}")
    print(f"     Total recusas (DISTINCTCOUNT — CORRETO): {total_distinct_r:>6,}")
    print(f"     Sobre-contagem:                         {total_count_r - total_distinct_r:>+6,}")

    if total_count > total_distinct:
        print(f"\n  ✅ CONFIRMADO: A correção de COUNT → DISTINCTCOUNT resolve a discrepância!")
        print(f"     Os valores DISTINCTCOUNT devem bater com a página 'Visão auditoria'.")
    else:
        print(f"\n  ℹ️  Os valores são iguais — a discrepância pode ter outra causa.")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Validação COUNT vs DISTINCTCOUNT — Formulários Preço Parceiro"
    )
    parser.add_argument(
        "--excel",
        type=str,
        default=None,
        help='Caminho para o arquivo "Projeto Preço Parceiro.xlsx" (opcional)',
    )
    args = parser.parse_args()

    print("=" * 80)
    print("  VALIDAÇÃO DE DADOS — Painel Preço Parceiro")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 80)

    try:
        conn = get_databricks_connection()
        df_fact = load_fact_data(conn)
        df_form = load_formulario(args.excel)

        run_validation(df_fact, df_form)

        conn.close()
        print("\n🔒 Conexão Databricks encerrada.")

    except ImportError as e:
        print(f"\n❌ Dependência faltando: {e}")
        print("   Instale com: pip install databricks-sql-connector pandas openpyxl")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
