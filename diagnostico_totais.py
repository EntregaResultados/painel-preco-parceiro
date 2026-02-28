"""
Diagnóstico rápido: Verificar se existem OS no formulário que aparecem
sob múltiplos clientes na FactAprovacaoPrecoParceiro.

Isso explicaria por que DISTINCTCOUNT no Total do Power BI
não bate com a soma das linhas individuais.
"""

import os
import sys
import pandas as pd

# Reutilizar conexão do script principal
sys.path.insert(0, os.path.dirname(__file__))
from validar_formularios import get_databricks_connection, QUERY_FACT, load_formulario


def diagnostico():
    print("=" * 80)
    print("  DIAGNÓSTICO: OS compartilhadas entre clientes")
    print("=" * 80)

    conn = get_databricks_connection()

    print("\n📊 Carregando FactAprovacaoPrecoParceiro...")
    cursor = conn.cursor()
    cursor.execute(QUERY_FACT)
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    df_fact = pd.DataFrame(rows, columns=cols)
    cursor.close()
    print(f"   ✅ {len(df_fact):,} linhas")

    df_form = load_formulario()
    if df_form is None:
        print("❌ Sem formulário.")
        return

    # Converter para string
    df_fact["NumeroOS_str"] = df_fact["NumeroOS"].astype(str).str.strip()
    df_form["NumeroOS_str"] = df_form["Número da ordem"].astype(str).str.strip()

    # OS unique por cliente na Fact
    os_por_cliente = df_fact.groupby("NomeCliente")["NumeroOS_str"].apply(set).to_dict()

    # OS no formulário
    os_formulario = set(df_form["NumeroOS_str"].unique())

    # OS do formulário que estão na Fact
    os_form_na_fact = set()
    os_form_multi_cliente = {}

    for os_num in os_formulario:
        clientes_desta_os = []
        for cliente, os_set in os_por_cliente.items():
            if os_num in os_set:
                clientes_desta_os.append(cliente)
        if len(clientes_desta_os) > 1:
            os_form_multi_cliente[os_num] = clientes_desta_os
        if clientes_desta_os:
            os_form_na_fact.add(os_num)

    print(f"\n  📊 Resumo:")
    print(f"     OS no formulário: {len(os_formulario)}")
    print(f"     OS do formulário encontradas na Fact: {len(os_form_na_fact)}")
    print(f"     OS do formulário em MÚLTIPLOS clientes: {len(os_form_multi_cliente)}")

    if os_form_multi_cliente:
        print(f"\n  ⚠️ OS compartilhadas entre clientes ({len(os_form_multi_cliente)}):")
        print(f"  {'OS':<15} {'Clientes'}")
        print("  " + "-" * 70)
        for os_num, clientes in sorted(os_form_multi_cliente.items()):
            print(f"  {os_num:<15} {', '.join(clientes)}")
    else:
        print("\n  ✅ Nenhuma OS compartilhada entre clientes.")
        print("     O problema de totalização pode ser outro.")

    # Simular: Total DISTINCTCOUNT global vs soma por cliente
    # Filtrar apenas respostas de OS que estão na Fact
    df_form_in_fact = df_form[df_form["NumeroOS_str"].isin(os_form_na_fact)]

    # Global DISTINCTCOUNT
    global_distinct = df_form_in_fact["NumeroOS_str"].nunique()

    # Soma de DISTINCTCOUNT por cliente
    # Para cada cliente, filtrar OS que pertencem a ele
    soma_distinct = 0
    for cliente, os_set in os_por_cliente.items():
        os_form_cliente = df_form_in_fact[df_form_in_fact["NumeroOS_str"].isin(os_set)]
        dc = os_form_cliente["NumeroOS_str"].nunique()
        if dc > 0:
            soma_distinct += dc

    print(f"\n  📊 Comparação Totais:")
    print(f"     DISTINCTCOUNT global (o que o Total do PBI mostra): {global_distinct}")
    print(f"     Soma dos DISTINCTCOUNT por cliente: {soma_distinct}")
    print(f"     Diferença: {soma_distinct - global_distinct}")

    if soma_distinct > global_distinct:
        print(f"\n  💡 EXPLICAÇÃO: {soma_distinct - global_distinct} OS aparecem em múltiplos clientes,")
        print(f"     causando a diferença entre a soma das linhas e o total.")

    # Recusas
    if "EC aceitou a negociação?" in df_form_in_fact.columns:
        df_recusas = df_form_in_fact[df_form_in_fact["EC aceitou a negociação?"] == "Não"]
        global_recusas = df_recusas["NumeroOS_str"].nunique()

        soma_recusas = 0
        for cliente, os_set in os_por_cliente.items():
            os_rec_cliente = df_recusas[df_recusas["NumeroOS_str"].isin(os_set)]
            dc = os_rec_cliente["NumeroOS_str"].nunique()
            if dc > 0:
                soma_recusas += dc

        print(f"\n  📊 Recusas:")
        print(f"     DISTINCTCOUNT global recusas: {global_recusas}")
        print(f"     Soma DISTINCTCOUNT recusas por cliente: {soma_recusas}")
        print(f"     Diferença: {soma_recusas - global_recusas}")

    conn.close()
    print("\n🔒 Conexão encerrada.")
    print("=" * 80)


if __name__ == "__main__":
    diagnostico()
