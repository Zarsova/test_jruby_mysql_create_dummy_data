# -*- coding: utf-8 -*-
#!/usr/bin/env ruby
$:.unshift File.dirname(__FILE__)
$:.unshift File.join(File.dirname(__FILE__), 'lib')
require 'yaml'
require 'common'
require 'tables'
require 'rubygems'
require 'jruby_threach'

# Eclipse
#  DLTK
#   url: http://download.eclipse.org/technology/dltk/updates/
#  yedit
#   url: http://dadacoalition.org/yedit
# IntelliJ IDEA
#  Ruby Plugin
#   url: http://confluence.jetbrains.com/display/RUBYDEV/RubyMine+and+IntelliJ+IDEA+Ruby+Plugin

class CreateDummyData
  DEBUG = false
  include SequelDatabase, DummyTables
  VERSION = '0.3.0'
  DEFAULT_MULTI_INSERT = 1250 # 1コミットで反映する行数
  DEFAULT_CHECK_POINT= 50000 # insertの状況をログ出力する行数(テーブルごと)
  DEFAULT_POOL_SIZE = 6 # threach の並列実行数

  def initialize(config, log = nil)
    @config = config
    @log = log ||= @config['log']
    if @log.nil?
      error_proc["Error: not found logger"]
    end
    Log::default_logger=@log
  end

  def process(env = nil, dry_run = nil)
    @log.info("init_process: start")
    @env = env ||= @config['base_env']
    @dry_run = dry_run

    @start_time = Time.now
    @total = 0
    ## read database.yml
    @target_db = db_config_proc[@config['database_yml'], @env]
    ## read values dir
    if @config.has_key?('values_dir')
      if @config['values_dir'].nil?
        error_proc["Error: values dir is not specified"]
      end
      if FileTest::directory?(@config['values_dir'])
        @log.info "found value dir: #{@config['values_dir']}"
        DummyTables::const_set(:DEFAULT_VALUES_DIR, @config['values_dir'])
      else
        error_proc["Error: not found values dir: #{@config['values_dir']}"]
      end
    end
    ## read tables.yml
    reference_schema, tables = tables_config_proc[@config['tables_yml']]
    @reference_db = db_config_proc[@config['database_yml'], reference_schema]
    @log.info("init_process: done")

    ## start initialization database
    @m = Mutex.new

    ## create DummyTable Array
    # スキーマ名・テーブル名の一覧を取得する
    puts @target_db.tables
    puts @reference_db.tables

    reference_tables = @reference_db.tables
    # 設定ファイルから取得した tables とデータベースから取得した tables_set を突合わせて target_tables を生成する
    target_tables = Array.new
    #tables_set.threach(DEFAULT_POOL_SIZE) do |row|
    reference_tables.each do |reference_table|
      tables.each do |table|
        if "#{reference_table}" == table.table_name
          # DummyTable にデータベースから取得した columns テーブルの情報(InformationSchemaColumns)を入力する
          require 'pp'
          pp @reference_db.schema(reference_table)
          @target_db.create_table(@reference_db.schema(reference_table))
          puts @target_db.schema(reference_table)
          @m.synchronize { target_tables << table }
        end
      end
    end

    target_tables.threach(DEFAULT_POOL_SIZE) do |table|
      #target_tables.each do |table|
      table_start_time = Time.now
      table_count = 0
      begin
        if @target_db.table_exists?(table.table_name)
          count_msg = current_row_num = @target_db[Sequel.identifier(:"#{table.table_name}")].count
        else
          current_row_num = 0
          count_msg = 'NaN'
        end
        table.current_count = current_row_num
        unique_msg = table.unique_groups.each_value.inject("") { |ret_msg, v| ret_msg << ", #{v}" }
        @log.info("target table: #{table.table_name}, count(cur/to): #{count_msg}/#{table.target_count}#{unique_msg}")

        current_row_num = table.current_count

        # @target_db << table.get_alter_table_sql('Innodb', 'Compressed') unless @dry_run
        until (current_row_num >= table.target_count)
          next_insert = DEFAULT_MULTI_INSERT
          if table.target_count - current_row_num < next_insert
            next_insert = table.target_count - current_row_num
          end
          if @dry_run
            (0..next_insert - 1).each.map do
              table.get_insert_array()
            end
          else
            insert_ds = @target_db[Sequel.identifier(:"#{table.table_name}")]
            insert_ds.insert_ignore.import(
                table.get_column_array(),
                (0..next_insert - 1).each.map do
                  table.get_insert_array()
                end
            )

          end
          table_count += next_insert
          current_row_num += next_insert
          @m.synchronize { @total += next_insert }
          @log.debug("insert: #{table.table_name}" << ",  rows: #{current_row_num}/#{table.target_count}" <<
                         ", rows/sec(total): #{rows_per_sec(table_count, table_start_time)}" <<
                         " (#{rows_per_sec(@total, @start_time)})")

          if (current_row_num % DEFAULT_CHECK_POINT < next_insert) && (current_row_num < table.target_count)
            @log.info("insert: #{table.table_name}" << ", rows: #{current_row_num}/#{table.target_count}" <<
                          ", rows/sec(total): #{rows_per_sec(table_count, table_start_time)}" <<
                          " (#{rows_per_sec(@total, @start_time)})")
          end
        end
        if @dry_run
          @log.info("inserted: #{table.table_name}" <<", rows: #{current_row_num}" <<
                        ", rows/sec(total): #{rows_per_sec(table_count, table_start_time)}" <<
                        " (#{rows_per_sec(@total, @start_time)})")
        end
        unless @dry_run
          table_status = @target_db.fetch("show table status like '#{table.table_name}'").first
          @log.info("result: #{table.table_name}" <<
                        ", count(cur/to): #{table_status[:rows]}/#{current_row_num}" <<
                        ", data: #{byte_2_mb_str table_status[:data_length]}" <<
                        ", index: #{byte_2_mb_str table_status[:index_length]}" <<
                        ", engine: #{table_status[:engine]}/#{table_status[:row_format]}")
        end
        table.close_variables
      rescue Sequel::DatabaseError => sqlError
        @log.warn("table: #{table.table_name}" <<", message: #{sqlError}")
      end
    end
    @log.info "spent total:#{sprintf("%.2f", Time.now - @start_time)}(sec)" <<", rows/sec: #{rows_per_sec(@total, @start_time)}" <<
                  ", insert total: #@total"
  end

  def rows_per_sec(rows, start_time)
    sprintf("%.2f", rows/(Time.now - start_time))
  end

  def byte_2_mb_str(byte)
    sprintf("%.2fMB", (byte / 1024.0 / 1024.0))
  end

  def merge_alter_table
    lambda do |pk, idx|
      qry = nil
      if pk.nil? && idx.nil?
        qry = nil
      elsif  pk.nil?
        qry = idx
      elsif  idx.nil?
        qry = pk
      else
        qry = pk + idx.gsub(/ALTER TABLE `.*` ([A|D][D|R][D|O])/, ', \1')
      end
      qry
    end
  end


  attr_accessor :log
end
if __FILE__ == $0
  config_file = BatchConfig.default_config_file
  dry_run = BatchConfig.default_dry_run
  env = nil

  require 'optparse'
  opts = OptionParser.new do |opts|
    opts.banner = "Mysql Create Dummy Data"
    opts.define_head "Usage: #{File.basename($0)} [options]"
    opts.separator "Options:"
    opts.on_tail("-h", "-?", "--help", "Show this message") do
      puts opts
      exit
    end
    opts.on("-c", "--config CONFIG", "set configuration file") do |v|
      config_file = v
    end
    opts.on("-d", "--dry-run", "run but don\'t change target database") do
      dry_run = true
    end
    opts.on("-e", "--env ENV", "use environment config for target database") do |v|
      env = v
    end
    opts.on_tail("-v", "--version", "Show version") do
      puts "#{File.basename($0, ".rb")} #{CreateDummyData::VERSION}"
      exit
    end
  end
  opts.parse!

  include BatchConfig
  config = batch_config[config_file]
  env ||= config['base_env'] ||= BatchConfig.default_env
  dry_run_message = dry_run ? ' with dry_run' : ''
  env_message = env ? ", env: #{env}" : ''
  batch = CreateDummyData.new(config)
  batch.log.info "#{File.basename($0, ".rb")}: start#{dry_run_message}#{env_message}"
  batch.process(env, dry_run)
  batch.log.info "#{File.basename($0, ".rb")}: end#{dry_run_message}#{env_message}"
end
