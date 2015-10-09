
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-azure_blob_storage"
  spec.version       = "0.1.0"
  spec.authors       = ["Satoshi Akama"]
  spec.summary       = %[Microsoft Azure blob Storage file input plugin for Embulk]
  spec.description   = %[Reads files stored on Microsoft Azure blob Storage.]
  spec.email         = ["satoshiakama@gmail.com"]
  spec.licenses      = ["Apache-2.0"]
  spec.homepage      = "https://github.com/sakama/embulk-input-azure_blob_storage"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
