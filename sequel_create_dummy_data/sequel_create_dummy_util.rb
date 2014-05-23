# -*- coding: utf-8 -*-
#!/usr/bin/env ruby
$:.unshift File.dirname(__FILE__)
require 'pp'
require 'fileutils'
require 'logger'
require "psych"
require 'yaml'
require 'script/common'
require 'script/tables'
require 'rubygems'
require 'jruby_threach'
require 'set'

class CreateDummyUtil
  include SequelDatabase
  VERSION = '0.2.0'

  DEFAULT_POOL_SIZE = 20
  CREATE_TABLES_GROUP_BY_LIMIT = 10
  CREATE_TABLES_VALUE_LIMIT = 10
  FILENAME_SAMPLE_TABLES = 'conf/sample_tables.yml'
  EXCLUDE_COLUMNS = %w(created_user created_date updated_user updated_date)
  INDENT = 50

  def initialize(config, log = nil)
    @config = config
    @log = log ||= config['log']
  end

  def read_database_config
    database_config = YAML::load_file(@config['database_yml'])
    if database_config.nil?
      @log.error("dump config: #{YAML::dump(@config)}")
      error_proc["Error: not found database_yml from #{@config['database_yml']}"]
    end
    if database_config[@env].nil?
      @log.error("dump config: #{YAML::dump(@config)}")
      @log.error("dump env: #{YAML::dump(@env)}")
      @log.error("dump db: #{YAML::dump(database_config)}")
      error_proc["Error: not found profile [#@env] from #{@config['database_yml']}"]
    end
    database_config
  end

  def process(env = nil, dry_run = nil)
    @env = env ||= @config['base_env']
    @dry_run = dry_run
    @start_time = Time.now
    @total = 0

    # read database.yml
    database_config = read_database_config()

    # start initialization database
    target_db = sequel_proc[database_config, @env]
    @mutex = Mutex.new

    @log.info "create conf file: #{FILENAME_SAMPLE_TABLES}"
    tables = []
    target_db.tables.threach(DEFAULT_POOL_SIZE) do |table|
      #target_db.tables.each do |table|
      begin
        table_hash = {}
        table_hash['name'] = "#{table}"
        table_hash['count'] = 0
        table_hash['from'] = 1
        table_hash['columns'] = []
        primary_keys = target_db.schema(Sequel.identifier(:"#{table}")).select do |_, col_setting|
          col_setting[:primary_key]
        end.map do |col_name, _|
          col_name
        end
        auto_increment = target_db.schema(Sequel.identifier(:"#{table}")).select do |_, col_setting|
          col_setting[:auto_increment]
        end.map do |col_name, _|
          col_name
        end

        @log.info "search table_name: #{table}, " <<
                      "primary keys: {#{primary_keys.join(', ')}}, " <<
                      "auto increment: {#{auto_increment.join(', ')}}"
        target_db[Sequel.identifier(:"#{table}")].columns.each do |column|
          if primary_keys.include?(column) then
            if auto_increment.include?(column) then
              column_hash = {'name' => "#{column} # primary_keys and auto_increment"}
            else
              column_hash = {'name' => "#{column} # primary_keys"}
            end
          elsif auto_increment.include?(column) then
            column_hash = {'name' => "#{column} # auto_increment"}
          else
            unless EXCLUDE_COLUMNS.include?("#{column}")
              array = Array::new
              qry = "select distinct #{column} as v from #{table}"
              target_db.fetch(qry).all.each do |row|
                row.each_pair do |_, v|
                  if v.instance_of?(String) || v.instance_of?(Sequel::SQL::Blob)
                    if v.size > 0
                      array << v
                    end
                  elsif v.instance_of?(Time)
                    array << v
                  elsif v.instance_of?(Date)
                    array << v
                  elsif v.instance_of?(FalseClass)
                    array << 0
                  elsif v.instance_of?(TrueClass)
                    array << 1
                  elsif v.instance_of?(BigDecimal)
                    array << v.to_f
                  elsif v.instance_of?(Float)
                    array << v
                  elsif v.instance_of?(Fixnum)
                    array << v
                  end
                end
              end
              @mutex.synchronize do
                @total += 1
              end
              if not array.nil? or array.size > 0 or not array.empty?
                array = array[0..CREATE_TABLES_VALUE_LIMIT-1]
                case array
                  when [0]
                    column_hash = {'name' => "#{column}", 'value' => 'zero_int'}
                  when ["0"]
                    column_hash = {'name' => "#{column}", 'value' => 'zero_int'}
                  when [0.0]
                    column_hash = {'name' => "#{column}", 'value' => 'zero_double'}
                  when ["0.0"]
                    column_hash = {'name' => "#{column}", 'value' => 'zero_double'}
                  when []
                    column_hash = {'name' => "#{column}", 'value' => 'blank_char'}
                  else
                    column_hash = {'name' => "#{column}", 'value' => array}
                end
              end
              if column_hash['value'].nil?
                column_hash.delete('value')
                column_hash = {'name' => "#{column}", 'value' => 'null_nontype'}
              elsif column_hash['value'].empty?
                column_hash.delete('value')
                column_hash = {'name' => "#{column}", 'value' => 'null_nontype'}
              end
            end
          end
          unless column_hash.nil?
            table_hash['columns'] << column_hash
          end
        end
        unless table_hash['columns'].size > 0
          table_hash.delete('columns')
        end
        @mutex.synchronize do
          tables << table_hash
        end
      rescue Sequel::DatabaseError => ex
        #if ex.to_s.index('java.sql.Timestamp')
        #  @log.error "Error: illegal timestamp table_name: #{table}"
        #else
        @log.error "Error: Sequel::DatabaseError => #{ex}"
        #end
      end
    end
    tables = tables.sort { |x, y| x['name'] <=> y['name'] }

    yamls = tables.inject("tables:\n") do |out_yaml, table_hash|
      raw_yaml = table_hash.to_yaml #(cannical = false)
      tmp_step1 = convert_yaml_list2ary_step1(raw_yaml)
      temp_step2 = convert_yaml_list2ary_step2(tmp_step1)
      begin
        out_yaml << convert_yaml_type(convert_yaml_add_comment(temp_step2))
      rescue NoMethodError => e
        out_yaml << temp_step2
      end
    end

    @log.info "create values sample: "<<FILENAME_SAMPLE_TABLES
    open(FILENAME_SAMPLE_TABLES, 'w') do |out_file|
      yamls.each_line do |line|
        out_file.write(line)
      end
    end
    @log.info "create file: "<<FILENAME_SAMPLE_TABLES<<
                  ", spent total:#{sprintf("%.2f", Time.now - @start_time)}(sec)"<<
                  ", qps: #{sprintf("%.2f", @total/(Time.now - @start_time))}"
    target_db.disconnect
  end

  def get_value_name(column_name, column_type)
    column_name + "_" + column_type.gsub(/[\(|\)|,]/, '_').gsub(/varchar/, 'char')
  end

  def convert_yaml_list2ary_step1(input)
    out = ""
    input.each_line do |line|
      if line == "---\n"
        line = "  - \n"
      else
        line = ["    ", line].join('')
      end
      out = out + line
    end
    out
  end

  def convert_yaml_list2ary_step2(input)
    out = ""
    next_line = ''
    input.each_line do |line|
      line = [next_line, line].join('')
      next_line = ''
      if / -  / =~ line
        line = line.gsub(/ - +/, ' - ')
        out = out + line
      elsif  / - \n/ =~ line
        next_line = line.gsub(/\n/, '').gsub(/\r/, '')
      else
        out = out + line
      end
    end
    out
  end

  def convert_yaml_add_comment(input)
    out = ""
    input.each_line do |line|
      if line.include?("name:") then
        line = line.gsub(/'/, "")
      end
      tmp = line
      if line.include?('# primary_keys from values') then
        tmp = line.sub(' # primary_keys from values', '    ## primary_keys from values')
      elsif line.include?('# from values') then
        tmp = line.sub(' # from values', '    ## from values')
      elsif line.include?('primary_keys') then
        tmp = line.sub('  - name: ', '  # - name: ').
            sub(' # primary_keys', '    ## primary_keys')
      elsif line.include?('auto_increment') then
        tmp = line.sub('  - name: ', '  # - name: ').
            sub(' # auto_increment', '    ## auto_increment')
      elsif line.include?('null_values') then
        tmp = line.sub('  - name: ', '  # - name: ').
            sub(' # null_values', '    ## null_values')
      end
      tmp = tmp.sub(/\"\[(.*)\]\"/, "[\1]")
      unless tmp.index('##').nil? then
        line_size = tmp.index('##')
        if line_size <= INDENT then
          tmp = tmp.sub('##', '##'.rjust(INDENT-line_size))
        end
      end
      out = out + tmp
    end
    out
  end

  def convert_yaml_type(input)
    out = ""
    c = Column.new
    input.each_line do |line|
      if /^    .*- name:/ =~ line then
        # カラムの場合
        out << c.yml_str unless c.name.nil?
        c = Column.new
        if /    # - name/ =~ line then
          # コメントアウトされている場合
          c.enable = false
        end
        c.name = line.match(/name: ([^ ]+)[\n| ]/)[1]
        # コメントが追加されている場合
        if /##/ =~ line then
          c.comment = line.match(/name: .* ## (.*)/)[1]
        end
      elsif /^      value:/ =~ line then
        # valueの場合
        if /^      value: (.+)/ =~ line then
          # ファイルの参照の場合
          c.value = line.match(/      value: (.+)/)[1]
        else
          c.value = []
        end
      elsif /^      - / =~ line then
        # 値がArrayで登録されている場合
        c.value << line.match(/      - (.*)/)[1]
      elsif /^        / =~ line then
        # 値がArrayで登録されている場合つづき
        # do nothing
      else
        out << c.yml_str unless c.name.nil?
        c = Column.new
        out << line
      end
    end
    out
  end

  attr_accessor :log
end


class Column
  def initialize
    @enable = true
    @name = nil
    @value = nil
    @comment = ""
  end

  def yml_str
    return_str = "      - "
    return_str = "      # - " unless @enable
    return_str << "{name: #{@name}"
    return_str = indent_str(return_str, 48)
    if @value.instance_of?(Array) then
      return_str << ", value: [#{@value.join(', ')}]"
    elsif @value.instance_of?(String) then
      return_str << ", value: #{@value.to_s}"
    else
      return_str << ", value: NaN"
    end
    return_str << "}"
    return_str = indent_str(return_str, 89)
    return_str << " ## #{@comment}" if @comment.size > 0
    return_str.gsub(/ +$/, '') + "\n"
  end

  attr_accessor :enable, :name, :value, :comment
end

def indent_str(str, num)
  if str.size < num then
    return str << " " * (num - str.size)
  else
    return str
  end
end

if __FILE__ == $0
  config_file = nil
  env = nil
  dry_run = nil
  require 'optparse'
  opts = OptionParser.new do |opts|
    opts.banner = "Mysql Create Dummy Util"
    opts.define_head "Usage: #{File.basename($0)} [options]"
    opts.separator "Options:"
    opts.on_tail("-h", "-?", "--help", "Show this message") do
      puts opts
      exit
    end
    opts.on("-e", "--env ENV", "use environment config for target database") do |v|
      env = v
    end
    opts.on_tail("-v", "--version", "Show version") do
      puts "#{File.basename($0, ".rb")} #{CreateDummyUtil::VERSION}"
      exit
    end
  end
  opts.parse!
  include BatchConfig
  config = batch_config[config_file]
  env_message = env ? ", env: #{env}" : ''
  batch = CreateDummyUtil.new(config)
  batch.log.info "#{File.basename($0, ".rb")}: start#{env_message}"
  batch.process(env, dry_run)
  batch.log.info "#{File.basename($0, ".rb")}: end#{env_message}"
end
