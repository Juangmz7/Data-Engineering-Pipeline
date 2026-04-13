output "storage_connection_string" {
  value       = azurerm_storage_account.storage_account.primary_connection_string
  sensitive   = true
  description = "Connection string to use in the Python script"
}

