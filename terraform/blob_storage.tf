resource "azurerm_storage_account" "storage_account" {
  name                     = "${var.storage_account_name}${random_integer.ri.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"

  # Set to 'Cool' since the use case involves writing a lot of data and reading infrequently.
  access_tier              = "Cool"

  tags = {
    environment = "data-pipeline"
    purpose     = "data-processed-storage"
  }
}

resource "azurerm_storage_container" "blob_container" {
  name                  = var.blob_container_name
  storage_account_id    = azurerm_storage_account.storage_account.id
  container_access_type = "private"
}