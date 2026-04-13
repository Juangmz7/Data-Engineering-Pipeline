resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.resource_group_location
}

resource "random_integer" "ri" {
  min = 10000
  max = 99999
}