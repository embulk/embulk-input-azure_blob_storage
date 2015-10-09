Embulk::JavaPlugin.register_input(
  "azure_blob_storage", "org.embulk.input.azure_blob_storage.AzureBlobStorageFileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
