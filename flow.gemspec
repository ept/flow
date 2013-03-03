Gem::Specification.new do |s|
  # The name 'flow' on rubygems is currently taken by a web server by Ryan Dahl.
  # It had just one release, in 2008. Hoping Ryan will give me the name.
  s.name = 'flow'
  s.version = '0.1.0'
  s.platform = Gem::Platform::RUBY
  s.author = 'Martin Kleppmann'
  s.email = 'martin@kleppmann.de'
  s.homepage = 'https://github.com/ept/flow'
  s.summary = 'Realtime, conflict-free synchronization of state'
  s.description = ''
  s.files = `git ls-files`.split("\n")
  s.require_path = 'lib'

  s.add_dependency 'avro'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'pry-rescue'
  s.add_development_dependency 'pry-stack_explorer'
end
