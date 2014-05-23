# -*- coding: utf-8 -*-
#!/usr/bin/env ruby
$:.unshift File.dirname(__FILE__)
require 'rubygems'
require 'sequel'
require 'yaml'
require 'logger'
require 'config'

module SequelDatabase
  Sequel.identifier_output_method = :downcase

  def sequel_proc
    Proc.new do |_database, _env|
      _env ||= "development"
      found_msg = ""
      db = if _database.nil? || _database.empty?
             Sequel.connect('mock:///')
           elsif _database.kind_of?(Hash) || File.exist?(_database)
             if _database.kind_of?(Hash)
               db_config = _database
             elsif File.exist?(_database)
               require 'yaml'
               db_config = YAML.load_file(_database)
             end

             db_config = db_config[_env] ||= db_config #  || db_config[env.to_sym]
             db_config.keys.each { |k| db_config[k.to_sym] = db_config.delete(k) }

             case db_config[:database]
               when 'mysql' then
                 require 'jars/mysql-connector-java-commercial-5.1.21-bin.jar'
                 additional_option = "?zeroDateTimeBehavior=convertToNull"

                 if db_config[:port].nil?
                   host = db_config[:host]
                 else
                   host = ([db_config[:host], db_config[:port]]).join(':')
                 end
                 options = db_config[:options]
                 unless db_config[:logger_dir].nil?
                   log = Logger.new(db_config[:logger_dir]+"/#{db_config[:host]}.#{db_config[:default_schema]}.database.log", 'daily')
                   log.level = eval(db_config[:log_level])
                   options = options.merge(
                       :logger => log)
                   log.info("#{db_config[:adapter]}:#{db_config[:database]}://#{host}/#{db_config[:default_schema]}#{additional_option}")
                 end
                 found_msg = "found database [#{_env}]" <<", database: #{db_config[:default_schema]}"<<", host: #{db_config[:host]}"<<", user: #{db_config[:options][:user]}"
                 Sequel.connect("#{db_config[:adapter]}:#{db_config[:database]}://#{host}/#{db_config[:default_schema]}#{additional_option}", options)
               when 'sqlserver' then
                 require 'lib/sqljdbc4.jar'
                 additional_option = ""

                 if db_config[:port].nil?
                   host = db_config[:host]
                 else
                   host = ([db_config[:host], db_config[:port]]).join(':')
                 end
                 options = db_config[:options]
                 unless db_config[:logger_dir].nil?
                   log = Logger.new(db_config[:logger_dir]+"/#{db_config[:host]}.#{db_config[:default_schema]}.database.log", 'daily')
                   log.level = eval(db_config[:log_level])
                   options = options.merge(
                       :logger => log)
                   log.info("#{db_config[:adapter]}:#{db_config[:database]}://#{host};databaseName=#{db_config[:default_schema]}")
                 end
                 found_msg = "found database [#{_env}]" <<", database: #{db_config[:default_schema]}"<<", host: #{db_config[:host]}"<<", user: #{db_config[:options][:user]}"
                 Sequel.connect("#{db_config[:adapter]}:#{db_config[:database]}://#{host};databaseName=#{db_config[:default_schema]}", options)
               else

             end
           else
             Sequel.connect(_database)
           end
      db.test_connection
      @log.info(found_msg)
      db
    end
  end

  def db_config_proc
    Proc.new do |_db_config, _env|
      extend BatchConfig, Log
      @log ||= Log.default_logger
      _env ||= BatchConfig.default_env
      unless File.exist?(_db_config)
        @log.error("dump config: #{YAML::dump(@config)}") unless @config.nil?
        error_proc["Error: not found database_yml from #{_db_config}"]
      end
      database_config = YAML::load_file(_db_config)
      if database_config.nil?
        @log.error("dump config: #{YAML::dump(@config)}") unless @config.nil?
        error_proc["Error: not found database_yml from #{_db_config}"]
      end
      if database_config[_env].nil?
        @log.error("dump config: #{YAML::dump(@config)}") unless @config.nil?
        @log.error("dump env: #{YAML::dump(_env)}") unless _env.nil?
        @log.error("dump db: #{YAML::dump(database_config)}")
        error_proc["Error: not found profile [#{_env}] from #{_db_config}"]
      end
      tdb = sequel_proc[database_config, _env]
      @log.info("connect database [#{_env}], target_schema: #{database_config[_env][:default_schema]}")
      tdb
    end
  end

  module_function :sequel_proc, :db_config_proc
end
