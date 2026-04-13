variable "subscription_id" {
    type        = string
    description = "Azure subscription Id"
    sensitive   = true
}

variable "resource_group_name" {
    type        = string
    default     = "datapipelines-fp"
    description = "Name of the resource group"
    sensitive   = false
}

variable "resource_group_location" {
    type        = string
    default     = "uaenorth"
    description = "Location of the resource group"
    sensitive   = false
}

variable "storage_account_name" {
    type        = string
    default     = "pipelinedatastorage"
    description = "Name of the storage account (must be lowercase, and have no special characters)"
    sensitive   = false
}

variable "blob_container_name" {
    type        = string
    default     = "blob-data-container"
    description = "Name of the blob container"
    sensitive   = false
}