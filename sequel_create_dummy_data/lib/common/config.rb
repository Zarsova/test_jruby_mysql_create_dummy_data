# -*- coding: utf-8 -*-
#!/usr/bin/env ruby
$:.unshift File.dirname(__FILE__)
require 'logger'
require 'error'
require 'java'

# Log 関連モジュール
module Log
  @default_logger = Logger::new(STDOUT)
  @default_logger.level=Logger::DEBUG
  class << self
    attr_accessor :default_logger
  end

  def basename_log
    lambda do |log_dir, log_echo, log_level|
      if log_echo
        log = Logger.new(STDOUT)
      else
        log = Logger.new("#{File.join(log_dir, File.basename($0, ".rb"))}.log", 'daily')
      end
      log.level=log_level
      log
    end
  end

  module_function :basename_log
end

# config読込 関連モジュール
module BatchConfig
  @default_dry_run = false
  @default_config_file = 'conf/configuration.yml'
  @default_env = "development"
  class << self
    attr_accessor :default_dry_run
    attr_accessor :default_config_file
    attr_accessor :default_env
  end

  def batch_config
    include Log
    Proc.new do |config_file|
      config_file ||= BatchConfig.default_config_file
      if File.exist?(config_file)
        require 'yaml'
        config = YAML.load_file(config_file)
      else
        error_proc["Error: Not exists configuration file: #{config_file}"]
      end
      config['home_dir'] = File.expand_path('.')
      log_level = eval(config['log_level'])
      log = Log.basename_log[config['log_dir'], config['log_echo'], log_level]
      config['log'] = log
      config
    end
  end

  module_function :batch_config
end