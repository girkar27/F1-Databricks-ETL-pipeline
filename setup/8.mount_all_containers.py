# Databricks notebook source

# creating a function that takes the containter name and data lake name(storage account name) and mounts containers

def mount_adls(container_name, dl_name):
    client_id = "98538ce0-7ce6-4b49-bc7c-eee0f41c204c"
    tenant_id = "180242f5-30d3-49a3-af5f-1b263f3cf943"
    client_secret = "1q18Q~yq.madBEEMiTyAfD3DqZgFsWawB5pJ1cVg"
    
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    #check if the container is already mounteed, if yes unmount and then mount again
    for mount in dbutils.fs.mounts():
        if mount == f"/mnt/{dl_name}/{container_name}/":
            dbutils.fs.unmount( f"/mnt/{dl_name}/{container_name}/")

    #mounting adls    
    # abfss://<container_name>@<data lake name>.dfs.windows.net
    mount_point = "/nmt/<any structure>"
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{dl_name}.dfs.core.windows.net",
        mount_point = f"/mnt/{dl_name}/{container_name}/",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("raw", "dlakeformula1")

# COMMAND ----------

mount_adls("processed", "dlakeformula1")
mount_adls("presentation", "dlakeformula1")

# COMMAND ----------

