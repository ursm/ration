D = Steep::Diagnostic::Ruby

target :lib do
  signature 'sig'
  check 'lib'

  library 'logger', 'json', 'securerandom'

  collection_config 'rbs_collection.yaml'

  configure_code_diagnostics(D.default) do |hash|
    hash[D::UnannotatedEmptyCollection] = nil
  end
end
