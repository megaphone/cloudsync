require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "cloudsync"
    gem.summary = %Q{Sync files between various clouds or sftp servers.}
    gem.description = %Q{Sync files between various clouds or sftp servers. Available backends are S3, CloudFiles, and SFTP servers. Can sync, mirror, and prune.}
    gem.email = "cory.forsyth@gmail.com"
    gem.homepage = "http://github.com/megaphone/cloudsync"
    gem.authors = ["Cory Forsyth"]
    gem.add_dependency "right_aws", "~> 2.0.0"
    gem.add_dependency "cloudfiles", "~> 1.4.8"
    gem.add_dependency "commander", "~> 4.0.3"
    gem.add_dependency "net-ssh", "~> 2.0.19"
    gem.add_dependency "net-sftp", "~> 2.0.4"
    
    
    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/test_*.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "cloudsync #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
