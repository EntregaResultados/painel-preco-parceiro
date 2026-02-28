"""
Consulta direta nos dados do formulário + Fact para mostrar os valores CORRETOS.
Sem multiplicação, sem cross-filter — dados brutos.
"""
import os, sys, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from validar_formularios import get_databricks_connection, QUERY_FACT, load_formulario


def main():
    print("=" * 90)
    print("  CONSULTA DIRETA — Dados corretos de formulário por Cliente")
    print("=" * 90)

    conn = get_databricks_connection()

    # 1. Carregar Fact
    print("\n📊 Carregando Fact do Databricks...")
    cursor = conn.cursor()
    cursor.execute(QUERY_FACT)
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]
    df_fact = pd.DataFrame(rows, columns=cols)
    cursor.close()
    print(f"   ✅ {len(df_fact):,} linhas na Fact")

    # 2. Carregar Formulário
    df_form = load_formulario()
    if df_form is None:
        print("❌ Sem formulário. Abortando.")
        conn.close()
        return

    # 3. Normalizar OS
    df_fact["os_str"] = df_fact["NumeroOS"].astype(str).str.strip()
    df_form["os_str"] = df_form["Número da ordem"].astype(str).str.strip()

    # 4. Mapear OS → Cliente (usando DISTINCT OS da Fact)
    #    Uma OS deve pertencer a um único cliente
    os_cliente = df_fact[["os_str", "NomeCliente"]].drop_duplicates(subset=["os_str"])
    dups = os_cliente.groupby("os_str").size()
    os_multi = dups[dups > 1]
    if len(os_multi) > 0:
        print(f"\n   ⚠️ {len(os_multi)} OS aparecem em MÚLTIPLOS clientes:")
        for os_num in os_multi.index[:10]:
            clientes = os_cliente[os_cliente["os_str"] == os_num]["NomeCliente"].tolist()
            print(f"      OS {os_num}: {', '.join(clientes)}")
        # Manter apenas o primeiro cliente por OS
        os_cliente = os_cliente.drop_duplicates(subset=["os_str"], keep="first")
    else:
        print(f"\n   ✅ Cada OS pertence a um único cliente.")

    # 5. Formulários que existem na Fact
    df_form_in_fact = df_form.merge(os_cliente, on="os_str", how="inner")
    print(f"\n   📋 Formulários com match na Fact: {len(df_form_in_fact):,}")
    print(f"   📋 OS distintas com formulário: {df_form_in_fact['os_str'].nunique()}")
    print(f"   📋 Formulários sem match na Fact: {len(df_form) - len(df_form_in_fact):,}")

    # 6. Contar por cliente — CORRETO (sem multiplicação)
    # Total de respostas = número de LINHAS do formulário por cliente (COUNT)
    # Mas cada OS no formulário deve contar UMA vez por OS (DISTINCTCOUNT)

    print(f"\n\n  {'='*90}")
    print(f"  RESULTADO CORRETO — por NomeCliente")
    print(f"  {'='*90}")

    # Método A: COUNT de linhas do formulário por cliente
    count_por_cliente = df_form_in_fact.groupby("NomeCliente").agg(
        count_linhas=("os_str", "count"),
        distinctcount_os=("os_str", "nunique")
    ).reset_index().sort_values("count_linhas", ascending=False)

    # Recusas
    has_recusa = "EC aceitou a negociação?" in df_form_in_fact.columns
    if has_recusa:
        recusas = df_form_in_fact[df_form_in_fact["EC aceitou a negociação?"] == "Não"]
        count_recusas = recusas.groupby("NomeCliente").agg(
            count_recusas_linhas=("os_str", "count"),
            distinctcount_recusas_os=("os_str", "nunique")
        ).reset_index()
        count_por_cliente = count_por_cliente.merge(count_recusas, on="NomeCliente", how="left").fillna(0)

    print(f"\n  {'Cliente':<35} {'COUNT':>7} {'DISTINCT':>9} {'Rec_CNT':>8} {'Rec_DST':>8}")
    print(f"  {'-'*80}")

    for _, row in count_por_cliente.iterrows():
        nome = str(row['NomeCliente'])[:34]
        rec_cnt = int(row.get('count_recusas_linhas', 0))
        rec_dst = int(row.get('distinctcount_recusas_os', 0))
        print(f"  {nome:<35} {int(row['count_linhas']):>7,} {int(row['distinctcount_os']):>9,} {rec_cnt:>8,} {rec_dst:>8,}")

    # Totais
    total_count = int(count_por_cliente['count_linhas'].sum())
    total_distinct = int(count_por_cliente['distinctcount_os'].sum())

    if has_recusa:
        total_rec_cnt = int(count_por_cliente.get('count_recusas_linhas', pd.Series([0])).sum())
        total_rec_dst = int(count_por_cliente.get('distinctcount_recusas_os', pd.Series([0])).sum())
    else:
        total_rec_cnt = 0
        total_rec_dst = 0

    print(f"  {'-'*80}")
    print(f"  {'TOTAL (soma linhas)':<35} {total_count:>7,} {total_distinct:>9,} {total_rec_cnt:>8,} {total_rec_dst:>8,}")

    # Total global (o que o Power BI mostra no Total row)
    global_count = len(df_form_in_fact)
    global_distinct = df_form_in_fact['os_str'].nunique()
    if has_recusa:
        global_rec_count = len(recusas)
        global_rec_distinct = recusas['os_str'].nunique()
    else:
        global_rec_count = 0
        global_rec_distinct = 0

    print(f"  {'TOTAL (global)':<35} {global_count:>7,} {global_distinct:>9,} {global_rec_count:>8,} {global_rec_distinct:>8,}")

    print(f"\n  📊 Comparação:")
    print(f"     COUNT linhas — soma por cliente: {total_count} | global: {global_count} | diff: {total_count - global_count}")
    print(f"     DISTINCTCOUNT OS — soma por cliente: {total_distinct} | global: {global_distinct} | diff: {total_distinct - global_distinct}")

    if total_count == global_count:
        print(f"\n  ✅ COUNT: soma das linhas == total global (sem duplicação)")
    else:
        print(f"\n  ⚠️ COUNT: diferença de {total_count - global_count} (OS em múltiplos clientes)")

    if total_distinct == global_distinct:
        print(f"  ✅ DISTINCTCOUNT: soma por cliente == total global")
    else:
        print(f"  ⚠️ DISTINCTCOUNT: diferença de {total_distinct - global_distinct} (OS em múltiplos clientes)")

    # 7. Ring chart comparison
    if has_recusa:
        total_nao = len(df_form_in_fact[df_form_in_fact["EC aceitou a negociação?"] == "Não"])
        total_sim = len(df_form_in_fact[df_form_in_fact["EC aceitou a negociação?"] == "Sim"])
        print(f"\n  📊 Ring Chart (o que deveria mostrar):")
        print(f"     Não: {total_nao}")
        print(f"     Sim: {total_sim}")
        print(f"     Total: {total_nao + total_sim}")
        print(f"     % Recusa: {100*total_nao/(total_nao+total_sim):.2f}%")

    # 8. Total SEM filtro de supervisor (para comparar)
    print(f"\n\n  {'='*90}")
    print(f"  ATENÇÃO: O visual tem filtro de Supervisor ativo!")
    print(f"  Os totais acima são sem filtro de supervisor.")
    print(f"  Para comparar, preciso saber qual supervisor está selecionado.")
    print(f"  {'='*90}")

    # Mostrar supervisores disponíveis
    if "NomeEC" in df_fact.columns:
        uf_stats = df_fact.groupby("UFEC")["os_str"].nunique().reset_index()
        print(f"\n  OS por UF: {dict(zip(uf_stats['UFEC'], uf_stats['os_str']))}")

    conn.close()
    print("\n🔒 Conexão encerrada.")


if __name__ == "__main__":
    main()
