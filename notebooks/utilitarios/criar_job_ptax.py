# Databricks notebook source
# MAGIC %md
# MAGIC # Criar o job da 2ª atualização do portal (dólar PTAX)
# MAGIC Cria (ou ajusta) o job **"4- Atualizar Portal IM - PTAX"**: roda o mesmo `00_atualizar_portal` com o parâmetro
# MAGIC `rodada = ptax`, que pula Automotivo, 4i e Resumo IA e faz só PTAX + Export + commit. Assim o card do dólar
# MAGIC mostra o fechamento do dia sem esperar a rodada da manhã seguinte.
# MAGIC
# MAGIC - Copia cluster, e-mails e "rodar como" do job **"3- Atualizar Portal IM"** (se ele existir). Se não existir, usa o cluster do campo **cluster**.
# MAGIC - Agenda: dias úteis, no **horário** do campo (fuso de São Paulo). A PTAX de fechamento sai por volta de 13h10, então 13:30 dá folga
# MAGIC   (se a tabela `historico_completo_ptax` for atualizada por outro job, agende depois dele).
# MAGIC - Rode como a **Vanessa** (dona das pastas do portal).
# MAGIC
# MAGIC 1. Rode a 1ª célula e preencha os campos.
# MAGIC 2. Rode tudo com **simular = sim** e confira.
# MAGIC 3. Troque pra **simular = nao** e rode de novo.

# COMMAND ----------

# DBTITLE 1,Atualizar databricks-sdk
# MAGIC %pip install --upgrade databricks-sdk -q

# COMMAND ----------

# DBTITLE 1,Reiniciar Python
# O DBR 14.3 vem com databricks-sdk 0.1.6 (sem autenticação no notebook e sem jobs.Task); reinicia para carregar a versão nova
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("simular", "sim", ["sim", "nao"], "simular")
dbutils.widgets.text("horario", "13:30", "horário (HH:MM, São Paulo)")
dbutils.widgets.text("cluster", "", "cluster (nome, só se o job 3 não existir)")

# COMMAND ----------

import copy
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
SIMULAR = dbutils.widgets.get("simular") == "sim"
EU = spark.sql("select current_user()").first()[0]
NOTEBOOK = f"/Workspace/Users/{EU}/info-portal/notebooks/00_atualizar_portal"
JOB_BASE, JOB_NOVO = "3- Atualizar Portal IM", "4- Atualizar Portal IM - PTAX"

hh, mm = (int(x) for x in dbutils.widgets.get("horario").strip().split(":"))
agenda = jobs.CronSchedule(quartz_cron_expression=f"0 {mm} {hh} ? * MON-FRI", timezone_id="America/Sao_Paulo",
                           pause_status=jobs.PauseStatus.UNPAUSED)

w.workspace.get_status(NOTEBOOK)   # falha aqui se o notebook não estiver nessa pasta
tarefa = jobs.Task(task_key="atualizar_portal_ptax",
                   notebook_task=jobs.NotebookTask(notebook_path=NOTEBOOK, base_parameters={"rodada": "ptax"}),
                   timeout_seconds=45 * 60)

base = next(iter(w.jobs.list(name=JOB_BASE)), None)
if base:
    s = w.jobs.get(base.job_id).settings
    t0 = (s.tasks or [None])[0]
    if t0 is not None:
        tarefa.existing_cluster_id = t0.existing_cluster_id
        tarefa.job_cluster_key = t0.job_cluster_key
    job_clusters, emails, run_as = copy.deepcopy(s.job_clusters), s.email_notifications, s.run_as
    print(f"Copiando cluster/e-mails do job '{JOB_BASE}' (id {base.job_id}).")
else:
    nome = dbutils.widgets.get("cluster").strip()
    ids = [c.cluster_id for c in w.clusters.list() if c.cluster_name == nome]
    if not ids:
        raise ValueError(f"Job '{JOB_BASE}' não encontrado: preencha o campo 'cluster' com o nome exato de um cluster (não serverless).")
    tarefa.existing_cluster_id, job_clusters, emails, run_as = ids[0], None, jobs.JobEmailNotifications(on_failure=[EU]), None

campos = dict(name=JOB_NOVO, tasks=[tarefa], job_clusters=job_clusters, schedule=agenda,
              email_notifications=emails, run_as=run_as, max_concurrent_runs=1)

existente = next(iter(w.jobs.list(name=JOB_NOVO)), None)
print(f"{'Atualizar' if existente else 'Criar'} '{JOB_NOVO}': {NOTEBOOK} (rodada=ptax), dias úteis às {hh:02d}:{mm:02d} (São Paulo)")
if SIMULAR:
    print("Simulação: nada foi alterado. Troque 'simular' pra 'nao' e rode de novo.")
elif existente:
    w.jobs.reset(job_id=existente.job_id, new_settings=jobs.JobSettings(**campos))
    print(f"✅ Job atualizado (id {existente.job_id}).")
else:
    novo = w.jobs.create(**campos)
    print(f"✅ Job criado (id {novo.job_id}).")
