# -*- coding: utf-8 -*-
#!/usr/bin/env ruby
$:.unshift File.dirname(__FILE__)
require 'yaml'
require 'script/common'
require 'script/tables'
require 'rubygems'
require 'jruby_threach'
require 'kconv'
$KCODE = 'UTF8'

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
  VERSION = '0.2.10'
  DEFAULT_MULTI_INSERT = 1250 # 1コミットで反映する行数
  DEFAULT_CHECK_POINT= 50000 # insertの状況をログ出力する行数(テーブルごと)
  DEFAULT_POOL_SIZE = 6 # threach の並列実行数
  THRESHOLD_FOR_USING_HASH = 20000000 # PK及び、indexをDropする基準行数(廃止)

  def initialize(config, log = nil)
    @config = config
    @log = log ||= @config['log']
    if @log.nil?
      error_proc["Error: not found logger"]
    end
    Log::default_logger=@log
  end

  def process(env = nil, dry_run = nil, csv_store = nil, csv_store_dir = nil)
    @log.info("init_process: start")
    @env = env ||= @config['base_env']
    @dry_run = dry_run
    @csv_store = csv_store
    @csv_store_dir = csv_store_dir ||= 'dist'

    if @csv_store && @csv_store_dir
      unless File.exists?(csv_store_dir)
        error_proc["Error: not found csv store dir #{csv_store_dir}"]
      end
    end
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
    @log.info("init_process: done")

    ## start initialization database
    # general_log = OFF, slow_query_log = OFF とする
    @target_db << 'SET GLOBAL general_log = OFF' unless @dry_run
    @target_db << 'SET GLOBAL slow_query_log = OFF' unless @dry_run

    @m = Mutex.new

    ## create DummyTable Array
    # スキーマ名・テーブル名の一覧を取得する
    tables_set = @target_db[:information_schema__tables].
        select(:table_schema, :table_name).
        where(:table_schema => reference_schema).all
    # 設定ファイルから取得した tables とデータベースから取得した tables_set を突合わせて target_tables を生成する
    target_tables = Array.new
    tables_set.threach(DEFAULT_POOL_SIZE) do |row|
      #tables_set.each do |row|
      tables.each do |table|
        if row[:table_name] == (table.table_name)
          if DEBUG
            @target_db.drop_table?(:"#{table.table_name}".identifier)
            @log.info("DEBUG OPTION drop table: #{table.table_name}")
          end
          unless @target_db.table_exists?(:"#{table.table_name}".identifier)
            @target_db << table.create_table_sql
          end

          # DummyTable にデータベースから取得した columns テーブルの情報(InformationSchemaColumns)を入力する
          table.columns = @target_db[:information_schema__columns].
              select(:character_maximum_length, :data_type, :column_name, :column_type, :numeric_precision).
              where(:table_schema => row[:table_schema], :table_name => row[:table_name]).
              order(:ordinal_position).all.inject([]) do |ret, column_row|
            ret << InformationSchemaColumns::new(column_row[:character_maximum_length],
                                                 column_row[:data_type],
                                                 column_row[:column_name],
                                                 column_row[:column_type],
                                                 column_row[:numeric_precision])
          end
          @m.synchronize { target_tables << table }
        end
      end
    end
    target_tables.threach(DEFAULT_POOL_SIZE) do |table|
      #target_tables.each do |table|
      begin
        big_table = false

        if @csv_store
          count_msg = current_row_num = 0
        elsif @target_db.table_exists?(table.table_name)
          count_msg = current_row_num = @target_db[:"#{table.table_name}".identifier].count
        else
          current_row_num = 0
          count_msg = 'NaN'
        end
        table.current_count = current_row_num
        unique_msg = table.unique_groups.each_value.inject("") { |ret_msg, v| ret_msg << ", #{v}" }
        @log.info("target table: #{table.table_name}, count(cur/to): #{count_msg}/#{table.target_count}#{unique_msg}")

        current_row_num = table.current_count
        if current_row_num < table.target_count
          if table.target_count > THRESHOLD_FOR_USING_HASH
            big_table = true #unless is_auto_increment[table.table_name]
          end
        end
        if big_table
          drop_pk_query = create_pk_query = nil
          if get_primary_key[table.table_name].size > 0 && !is_auto_increment[table.table_name]
            drop_pk_query = "ALTER TABLE `#{table.table_name}` DROP PRIMARY KEY"
            create_pk_query = alter_pk_query[table.table_name, get_primary_key[table.table_name], false]
          end
          drop_idx_query, create_index_query = drop_index[table.table_name]
          qry = merge_alter_table[drop_pk_query, drop_idx_query]
          unless qry.nil?
            @log.info("/* IF ERROR */ #{merge_alter_table[create_pk_query, create_index_query]}")
            @log.info("/* drop pk and idx */ #{qry}")
            @target_db << qry unless @dry_run
          end
        end

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
          elsif @csv_store
            buf = ""
            (0..next_insert - 1).each do
              buf = buf + (table.get_insert_array_csv().join(",") + "\n")
            end
            File::open("#{csv_store_dir}/#{table.table_name}.CSV", 'a') do |f|
              f.write(buf)
            end
          else
            insert_ds = @target_db[:"#{table.table_name}".identifier]
            insert_ds.insert_ignore.import(
                table.get_column_array(),
                (0..next_insert - 1).each.map do
                  table.get_insert_array()
                end
            )

          end
          current_row_num += next_insert
          @m.synchronize { @total += next_insert }
          @log.debug("insert: #{table.table_name}" << ",  rows: #{current_row_num}/#{table.target_count}" <<
                         ", rows/sec: #{rows_per_sec()}")

          if (current_row_num % DEFAULT_CHECK_POINT < next_insert) && (current_row_num < table.target_count)
            @log.info("insert: #{table.table_name}" << ", rows: #{current_row_num}/#{table.target_count}" <<
                          ", rows/sec: #{rows_per_sec()}")
          end
        end
        if @dry_run
          @log.info("inserted: #{table.table_name}" <<", rows: #{current_row_num}" << ", rows/sec: #{rows_per_sec()}")
        end
        if big_table
          qry = merge_alter_table[create_pk_query, create_index_query]
          unless qry.nil?
            @log.info("/* create pk and idx */ #{qry}")
            @target_db << merge_alter_table[create_pk_query, create_index_query] unless @dry_run
          end
        end
        unless @dry_run
          table_status = @target_db.fetch("show table status like '#{table.table_name}'").first
          @log.info("result: #{table.table_name}" <<
                        ", count(cur/to): #{table_status[:rows]}/#{current_row_num}" <<
                        ", data: #{byte_2_mb_str table_status[:data_length]}" <<
                        ", index: #{byte_2_mb_str table_status[:index_length]}" <<
                        ", engine: #{table_status[:engine]}/#{table_status[:row_format]}")
        end
      rescue Sequel::DatabaseError => sqlError
        require "kconv"
        @log.warn("table: #{table.table_name}" <<", message: #{sqlError.to_s.kconv(Kconv::AUTO, Kconv::AUTO)}")
      end
    end
    #@target_db << 'SET GLOBAL general_log = OFF'
    @target_db << 'SET GLOBAL slow_query_log = ON'
    @log.info "spent total:#{sprintf("%.2f", Time.now - @start_time)}(sec)" <<", rows/sec: #{rows_per_sec()}" <<
                  ", insert total: #@total"
  end

  def rows_per_sec
    sprintf("%.2f", @total/(Time.now - @start_time))
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
  csv_store_dir = nil
  csv_store = nil


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
    opts.on("--csvdir", "csv files dist dir") do |v|
      csv_store_dir = v
    end
    opts.on("--csv", "create csv files") do
      csv_store = true
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
  batch.process(env, dry_run, csv_store, csv_store_dir)
  batch.log.info "#{File.basename($0, ".rb")}: end#{dry_run_message}#{env_message}"
end
