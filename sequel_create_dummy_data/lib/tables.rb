#!/usr/bin/env ruby
# -*- coding: utf-8 -*-
$:.unshift File.dirname(__FILE__)
require 'common'
require 'generator'
require 'yaml'
require 'set'

DEFAULT_SAMPLE_PRINT = 3
DEFAULT_VALUES_DIR = "tmp/values"

module DummyTables
  class DummyTable
    #_schema_name, _tbl_name,_tbl_count, _tbl_from,ret_columns
    def initialize(ref_schema, table_name, target_count, table_from, col_configs, log, current_count = 0)
      @log = log
      @table_name = table_name # レイアウトを参照するテーブル名
      @table_from = table_from # Insert開始行数
      @target_count = target_count # 目標とする行数
      @current_count = current_count # 現在の行数
      @ref_schema = ref_schema # レイアウトを参照するテーブルが所属するスキーマ
      @config_columns = col_configs # tables.yml から読み込んだ絡む情報
      @current_columns = {} # すべてのカラムオブジェクト
      @use_double_quote =[]

      @columns_order = [] # カラムの並び順
      @unique_groups = {} # カラムオブジェクトのうち unique 設定を持つカラム
      @depend_columns = [] # カラムオブジェクトのうち depend 設定を持つカラム
      @fix_columns = [] # カラムオブジェクトのうち固定値を持つカラム
      @func_columns = []
    end

    def current_count=(val)
      @current_count = val
      update_col_map()
    end

    def columns=(val)
      @information_schema_columns = val
    end

    def update_col_map
      @information_schema_columns.each { |column_information|
        @columns_order << column_information.column_name.to_sym
        _col = @config_columns[:"#{column_information.column_name}"]
        col = if _col.nil?
                dc = DummyColumn::new(column_information.column_name, @current_count + @table_from)
                dc.info = column_information
                dc.func = dc.gen_func
                @func_columns << dc
                dc
              else
                resolve_column(_col, column_information)
              end

        @current_columns[column_information.column_name.to_sym] = col
      }
      @config_columns.each_pair { |k, v|
        if k.to_s =~ /^ghost/
          v = resolve_column(v, InformationSchemaColumns::new(0, 'dummy', k.to_s, 'int(1)', 1))
          @current_columns[k.to_sym] = v
        end
      }

      # 初期化
      @unique_groups.each_pair { |k, v|
        v.index = (@current_count + @table_from -1 %v.total_length)
      }
    end

    def resolve_column(_col, column_info)
      if _col.instance_of?(Struct::Key)
        if @unique_groups.has_key?(_col.unique_name)
          @unique_groups[_col.unique_name] << _col
        else
          @unique_groups[_col.unique_name] = UniqueKey::new()
          @unique_groups[_col.unique_name] << _col
        end
        _col
      elsif _col.instance_of?(DependColumn)
        _col.func = _col.gen_func
        @depend_columns << _col
        _col
      elsif _col.instance_of?(DependXYColumn)
        _col.func = _col.gen_func
        @depend_columns << _col
        _col
      elsif _col.instance_of?(DummyColumn) #fromの指定
        _col = DummyColumn::new(column_info.column_name, @current_count + _col.from)
        _col.info = column_info
        _col.func = _col.gen_func
        @func_columns << _col
        _col
      elsif _col.instance_of?(GlobalValuesColumn) #fromの指定
        _col.from += (@current_count + @table_from -1)
        _col.func = _col.gen_func
        _col = check_fix_value(_col)
        _col
      elsif _col.instance_of?(ValueColumn) #fromの指定
        _col.from += (@current_count + @table_from -1)
        _col.func = _col.gen_func
        _col = check_fix_value(_col)
        _col
      end
    end

    def check_fix_value(col)
      if col.type == "Array" and col.value.size == 1
        ret = FixColumn.new(_col.func[])
        @fix_columns << ret
        ret
      else
        @func_columns << col
        col
      end
    end

    # depricated
    def create_table_sql()
      "create table `#@table_name` like `#@ref_schema`.`#@table_name`"
    end

    def get_insert_array()
      # x -> y ではないカラムの現在の値を取得
      @unique_groups.each_value { |v|
        if v.has_next?
          v.next
        else
          v.index=0
          v.next
        end
      }
      @func_columns.each { |_col|
        _col.current_value = _col.func[]
      }
      @depend_columns.each { |_col| _col.current_value = nil }
      # x -> y なカラムの現在の値を取得 3回まで依存解消
      3.times.each do
        @depend_columns.each { |_col|
          if  _col.current_value.nil?
            if _col.instance_of?(DependColumn)
              _x_col = @current_columns[_col.x]
              unless _x_col.current_value.nil?
                begin
                  _col.current_value = _col.func[_x_col.current_value]
                rescue ArgumentError => aError
                  msg = "x_col: #{_x_col.current_value}" <<", x: #{_col.x}" <<", depend_func: #{_col.depend_func}" << ", message: #{aError.to_s}"
                  @log.debug(msg)
                  raise()
                end
              end
            elsif _col.instance_of?(DependXYColumn)
              _x_col = @current_columns[_col.x]
              _y_col = @current_columns[_col.y]
              unless (_x_col.current_value.nil? or _y_col.current_value.nil?)
                begin
                  _col.current_value = _col.func[_x_col.current_value, _y_col.current_value]
                rescue ArgumentError => aError
                  msg = "x_col: #{_x_col.current_value}" <<", y_col: #{_y_col.current_value}" <<", x: #{_col.x}" <<", y: #{_col.y}" <<", depend_func: #{_col.depend_func}" << ", message: #{aError.to_s}"
                  @log.debug(msg)
                  raise()
                end
              end
            end
          end
        }
      end

      @depend_columns.each do |_col|
        if  _col.current_value.nil?
          if _col.instance_of?(DependColumn)
            _x_col = @current_columns[_col.x]
            msg = "dependence is unsolvable: x_col: #{_x_col.current_value}" <<
                ", x: #{_col.x}" <<
                ", depend_func: #{_col.depend_func}"
            @log.debug(msg)
          elsif _col.instance_of?(DependXYColumn)
            _x_col = @current_columns[_col.x]
            _y_col = @current_columns[_col.y]
            msg = "dependence is unsolvable: x_col: #{_x_col.current_value}" <<
                ", y_col: #{_y_col.current_value}" <<
                ", x: #{_col.x}" <<", y: #{_col.y}" <<
                ", depend_func: #{_col.depend_func}"
            @log.debug(msg)
          end
        end
      end
      ret = @columns_order.inject([]) { |ary, name|
        ary << @current_columns[name].current_value
      }
    end

    def get_insert_array_csv()
      if @use_double_quote.size == 0
        @information_schema_columns.each do |info|
          if info.data_type == 'timestamp' || info.data_type == 'datetime' ||
              info.data_type == 'time' || info.data_type == 'date'||
              info.data_type == 'varchar' || info.data_type == 'char'||
              info.data_type == 'text' || info.data_type == 'longtext'
            @use_double_quote << true
          else
            @use_double_quote << false
          end
        end
      end
      ary = get_insert_array()
      rtn_ary = []
      @use_double_quote.each_with_index do |use, i|
        if use
          rtn_ary << "\"#{ary[i]}\""
        else
          rtn_ary << ary[i]
        end
      end
      rtn_ary
    end

    def get_column_array()
      @columns_order
    end

    def get_alter_table_sql(engine, row_format = nil)
      sql = "alter table `#@table_name` engine=#{engine}"
      unless row_format.nil?
        sql = sql + " row_format=#{row_format}"
      end
      sql
    end

    def close_variables
      self.instance_variables.map do |sym|
        self.instance_variable_set(sym, nil)
      end
    end

    attr_accessor :table_name, :ref_schema, :target_count
    attr_reader :current_columns, :unique_groups, :current_count
  end

  Key = Struct::new("Key", :name, :unique_name, :iterator, :current_value)
  class UniqueKey < Array;
    @index = nil
    @current = Hash::new

    def total_length
      self.inject(1) { |length, key|
        length = length * key.iterator.length
      }
    end

    def has_next?
      init() if @index.nil?
      @index < self.total_length
    end

    def init
      @index = 0
      @current = Hash::new if @current.nil?
      update_current()
    end

    def update_current
      @current = Hash::new if @current.nil?
      self.inject(1) { |length, key|
        @current[key.name] = key.iterator.values[(@index/length) % key.iterator.length]
        length = length * key.iterator.length
      }
    end

    def next
      init() if @index.nil?
      update_current()
      values = @current.clone
      self.inject(1) { |length, key|
        key.current_value = key.iterator.values[(@index/length) % key.iterator.length]
        length = length * key.iterator.length
      }
      @index += 1
      values
    end

    def to_s
      "#{self[0].name.gsub(/\..*/, '')}(#{self.total_length}) [#{self.collect { |key| "#{key.name.gsub(/.*\./, '')}(#{key.iterator.length})" }.join(', ')}]"
    end

    @log
    attr_accessor :index, :log
  end

  def tables_config_proc
    lambda do |table_config|
      unless File.exists?(table_config)
        error_proc["Error: not found table_config from #{table_config}"]
      end
      _table_config = YAML::load_file(table_config)
      _schema_name = _table_config['schema_name']
      _default_engine = _table_config['default_engine']
      _default_innodb_rowformat = _table_config['default_innodb_rowformat']
      _tables = _table_config['tables']
      _values = _table_config['values']
      _uniques = _table_config['unique']
      _values = parse_value_conf(_values)

      ret_uniques = {}
      unless _uniques.nil?
        _uniques.each { |_unique|
          _unique_name = _unique['name']
          _unique_keys = _unique['keys']
          if _unique_name.nil? or _unique_keys.nil?
            error_proc["Error: unique key must have [name] and [keys], unique_name: #{_unique_name}"]
          end
          if not _unique_keys.instance_of?(Array) or _unique_keys.size < 1
            error_proc["Error: unique key must have one or multi [keys], unique_name: #{_unique_name}"]
          end

          tmp_unique_for_logging = UniqueKey::new()
          _unique_keys.each { |_key|
            _key_name = _key['name']
            _key_value = _key['value']
            if _key_name.nil? or _key_value.nil?
              error_proc["Error: key must have [name] and [value], unique_name: #{_unique_name}, key_name: #{_key_name}"]
            end
            gvc_for_key_value = _values[_key_value]
            if gvc_for_key_value.nil?
              error_proc["Error: can't find key_value from values, unique_name: #{_unique_name}, key_value: #{_key_name}, key_name: #{_key_value}"]
            end
            tmp_unique_for_logging << Key::new("#{_unique_name}.#{_key_name}", "#{_unique_name}", gvc_for_key_value.iterator())
            _values["#{_unique_name}.#{_key_name}"] = Key::new("#{_unique_name}.#{_key_name}", "#{_unique_name}", gvc_for_key_value.iterator())
          }
          @log.info("found unique key, #{tmp_unique_for_logging}")
          3.times.each {
            ary = []
            tmp_unique_for_logging.next.each_pair { |k, v|
              ary << "#{k.gsub(/.*\./, '')}:#{v}"
            }
            @log.info("#{_unique_name} [#{ary.join(', ')}]")
          }
          tmp_unique_for_logging.clear
        }
      end

      if _table_config.nil?
        @log.error("dump config: #{YAML::dump(@config)}")
        @log.error("dump table_config: #{YAML::dump(_table_config)}")
        error_proc["Error: not found table_config from #{table_config}"]
      end

      ret_tables = parse_table_conf(_schema_name, _tables, _values)
      @log.info("reference_schema: #{_schema_name}" <<
                    ", engine: #{_default_engine ? _default_engine : 'NaN'}" <<
                    ", innodb_raw_format: #{_default_innodb_rowformat ? _default_innodb_rowformat : 'NaN'}")
      return _schema_name, ret_tables
    end
  end

  def parse_table_conf(_schema_name, _tables, _values, include = true)
    ret_tables = []
    # include の解決
    if include
      _tables.reject do |_tbl|
        _tbl['include'].nil?
      end.each do |_tbl|
        _include = _tbl['include']
        unless File.exists?(_include)
          error_proc["Error: not found additional table_config from #{_include}"]
        end
        _include_config = YAML::load_file(_include)
        if _include_config.nil?
          @log.error("dump table_config: #{YAML::dump(_include_config)}")
          error_proc["Error: not found additional table_config from #{_include_config}"]
        end
        tmp = parse_table_conf(_schema_name, _include_config['tables'], _values, false)
        ret_tables.each do |dummy_table|
          tmp.each do |tmp_dummy_table|
            if tmp_dummy_table.table_name == dummy_table.table_name
              error_proc["Error: found duplicate table entry #{tmp_dummy_table.table_name} in #{_include}"]
            end
          end
        end
        ret_tables = ret_tables + tmp
      end
    end

    _tables.reject do |_tbl|
      _tbl['name'].nil?
    end.each do |_tbl|
      _tbl_name = _tbl['name']
      ret_tables.each { |dummy_table|
        if _tbl_name == dummy_table.table_name
          error_proc["Error: found duplicate table entry #{_tbl_name}"]
        end
      }
      _tbl_count = _tbl['count']
      _tbl_from = _tbl['from']
      _tbl_from = 1 if _tbl_from.nil? or _tbl_from == 0
      _tbl_columns = _tbl['columns']

      ret_columns = {}
      unless _tbl_columns.nil?
        _tbl_columns.each { |_col|
          _col_name = _col['name']
          _col_value = _col['value']
          _col_depend = _col['depend']
          _col_from = _col['from']
          _col_step = _col['step']
          ret_col = if not _col_value.nil? and not _col_from.nil?
                      error_proc["Error: [value] and [from] must not be same time for #{_tbl_name}.#{_col_name}"]
                    else
                      if _col_value.instance_of?(String)
                        # 検証済みのvaluesから設定している場合(unique含む)
                        unless _values.has_key?(_col_value)
                          error_proc["Error: not found [value] name #{_col_value} for #{_tbl_name}.#{_col_name}"]
                        end
                        @log.debug "#{_tbl_name}.#{_col_name}, type: Values, name: #{_col_value}"
                        _values[_col_value].clone
                      elsif not _col_from.nil?
                        # カラム独自のfronの場合
                        unless _col_from.instance_of?(Fixnum)
                          error_proc["Error: [from] is only Fixnum for #{_tbl_name}.#{_col_name}"]
                        end
                        @log.debug "#{_tbl_name}.#{_col_name}, type: From, start: #{_col_from}"
                        DummyColumn::new(_col_name, _col_from)
                      elsif not _col_depend.nil?
                        # 他のカラムの部分従属の場合 X → Y
                        if  _col_depend['x'].nil? or _col_depend['func'].nil?
                          error_proc["Error: [depend] must have [x] [func] for #{_tbl_name}.#{_col_name}"]
                        end
                        if _col_depend['y'].nil?
                          @log.debug "#{_tbl_name}.#{_col_name} has X->Y, X: #{ _col_depend['x']}, Func: #{_col_depend['func']}"
                          DependColumn::new(_col_name, _col_depend['x'], _col_depend['func'])
                        else
                          @log.debug "#{_tbl_name}.#{_col_name} has X,Y->Z, X: #{ _col_depend['x']}, Y: #{ _col_depend['y']}, Func: #{_col_depend['func']}"
                          DependXYColumn::new(_col_name, _col_depend['x'], _col_depend['y'], _col_depend['func'])
                        end
                      else
                        # カラム独自のvalueを設定している場合
                        vc = ValueColumn::new(_col_name, _col_value, _col_step)
                        @log.debug "#{_tbl_name}.#{_col_name}, type: #{vc.type} #{values_test(vc, 3)}"
                        vc
                      end
                    end
          ret_columns[:"#{_col_name}"] = ret_col
        }
      end
      ret_tables << DummyTable::new(_schema_name, _tbl_name, _tbl_count, _tbl_from, ret_columns, @log)
    end
    ret_tables
  end


  def parse_value_conf(_values)
    ret_values = {}
    unless _values.nil?
      _values.each { |_value|
        gvc = GlobalValuesColumn::new(_value['name'], _value['value'], _value['step'])
        @log.info "global.#{gvc.name}, type:#{gvc.type} #{values_test(gvc, DEFAULT_SAMPLE_PRINT)}"
        ret_values[_value['name']] =gvc
      }
    end
    ret_values
  end

  def values_test(val, num = 5, msg='sample')
    test = val.gen_func
    if test.nil?
      @log.error("dump config name: #{val.name}, value: #{val.value}")
      error_proc["Error: not found value file from #{val.name}"]
    end
    out = Array::new
    num.times.each {
      out << test[]
    }
    out_msg = "[" << out.join(', ') << ",..]"
    out_msg
  end


  class InformationSchemaColumns
    @length = nil

    def initialize(character_maximum_length, data_type, column_name, column_type, numeric_precision)
      @character_maximum_length = character_maximum_length
      @data_type = data_type
      @column_name = column_name.downcase
      @column_type = column_type
      @numeric_precision = numeric_precision
      _tmp_length_0 = @column_type.scan(/[0-9]+/)[0].to_i
      _tmp_length_1 = @column_type.scan(/[0-9]+/)[1].to_i
      _tmp_length_0 = _tmp_length_0 - _tmp_length_1 unless _tmp_length_1.nil?
      _tmp_length_0 = 10 if _tmp_length_0 > 10
      unless @numeric_precision.nil?
        if @numeric_precision < _tmp_length_0
          _tmp_length_0 = @numeric_precision
        end
      end
      @length = _tmp_length_0 unless _tmp_length_0.nil?
    end

    attr_accessor :length, :character_maximum_length, :data_type, :column_name, :column_type, :numeric_precision
  end

  module DummyColumnBaseMixin
    @current_value = nil
    @func = nil

    def initialize(name, from = 0)
      @name = name
      @from = from
    end

    attr_accessor :from, :name, :current_value, :func
  end

  module ValueColumnMixin
    include DummyColumnBaseMixin

    def initialize(name, value, step)
      super(name)
      @value = value
      @step = step ||= 1
      if @type.nil?
        @type = value.class
      end
    end

    def gen_func
      f = if @value.instance_of?(Array)
            if @value.nil? or @value.size == 0
              lambda do
                nil
              end
            elsif @value.size == 1
              lambda do
                @value[0]
              end

            else
              gen_loop_array(@value, @from)
            end
          elsif @value.instance_of?(Range)
            gen_loop_range(@step, @value, @from)
          else
            nil
          end
    end

    attr_accessor :value, :step, :type
  end

  class FixColumn
    def initialize(current_value)
      @current_value = current_value
    end
  end
# データベースからInformationスキーマの情報を受け取って
# ダミーデータを出力する
  class DummyColumn
    include DummyColumnBaseMixin
    @data_type
    @length
    @character_maximum_length

    def gen_func
      f = get_column_func(@data_type, @length, @character_maximum_length, @from)
    end

    def info=(val)
      @data_type = val.data_type
      @length = val.length
      @character_maximum_length = val.character_maximum_length
    end

    attr_accessor :data_type, :length, :character_maximum_length
  end
  class DependColumn
    include DummyColumnBaseMixin

    def initialize(name, depend_x, depend_func)
      super(name)
      @x = depend_x.to_sym
      @depend_func = depend_func
    end

    def gen_func
      s = <<EOS
hash = Hash::new
enable_hash = true
from_hash = 0
lambda do |x|
  if enable_hash && hash.has_key?(x)
    v = hash[x]
    from_hash += 1
  else
    v = #@depend_func
    hash[x] = v if enable_hash
  end
  if enable_hash && hash.size > 1024
    hash.clear
    enable_hash = false if from_hash == 0
  end
  v
end
EOS
      f = eval(s)
    end

    attr_accessor :x, :depend_func
  end

  class DependXYColumn
    include DummyColumnBaseMixin

    def initialize(name, depend_x, depend_y, depend_func)
      super(name)
      @x = depend_x.to_sym
      @y = depend_y.to_sym
      @depend_func = depend_func
    end

    def gen_func
      s = <<EOS
hash = Hash::new
enable_hash = true
from_hash = 0
lambda do |x, y|
  key = x.to_s + y.to_s
  if enable_hash && hash.has_key?(key)
    v = hash[key]
    from_hash += 1
  else
    v = #@depend_func
    hash[key] = v if enable_hash
  end
  if enable_hash && hash.size > 512
    hash.clear
    enable_hash = false if from_hash == 0
  end
  v
end
EOS
      f = eval(s)
    end

    attr_accessor :x, :y, :depend_func
  end

  class ValueColumn
    include ValueColumnMixin
  end

  class GlobalValuesColumn
    include ValueColumnMixin
    alias super_func gen_func

    def initialize(name, value, step)
      super(name, value, step)
      file_name = File.join('.', DEFAULT_VALUES_DIR, "#@name.dat")
      if File.exist?(file_name)
        @type= 'File'
        @file = file_name
        open(file_name) { |f|
          @file_values = f.inject(Array::new) { |ret_ary, line|
            line = line.gsub("\r", '').gsub("\n", '')
            if line.size > 0
              ret_ary << line
            end
          }
        }
        if @file_values.size == 0
          error_proc["Error: file is empty file_name: #{file_name}"]
        end
      end
    end

    def gen_func
      f = if @type == 'File'
            gen_loop_file(@file_values, @from)
          else
            super_func
          end
    end

    def iterator
      GlobalValuesColumnIterator::new(self)
    end

    attr_reader :file_values
  end

  class GlobalValuesColumnIterator
    def initialize(gvc)
      @index = 0
      if gvc.type == 'File'
        @values = gvc.file_values
      else
        if gvc.value.instance_of?(Array)
          @values = gvc.value
        elsif gvc.value.instance_of?(Range)
          @values = gvc.value.step(gvc.step).inject([]) { |ret, v| ret << v }
        end
      end
    end

    def current_value
      @values[@index]
    end

    def has_next?
      @index < @values.length
    end

    def next
      value = @values[@index]
      @index += 1
      value
    end

    def length
      @values.length
    end

    attr_reader :values
    attr_accessor :index
  end
end