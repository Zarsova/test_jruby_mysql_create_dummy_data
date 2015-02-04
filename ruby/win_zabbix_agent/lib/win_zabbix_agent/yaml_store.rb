require 'yaml/store'
require 'fileutils'

module WinZabbixAgent
  class YamlStore
    def initialize(logger = nil, file)
      @logger = logger || Logger.new(STDERR)
      # 親ディレクトリがなければ作成
      FileUtils.mkdir_p(File.dirname(file)) unless Dir.exist?(File.dirname(file))
      @file = file
      # Storeインスタンスの作成
      @db = YAML::Store.new(file)
    end

    #指定したkeyにvalueが保存されている場合  true を返す
    #読込時のYMLの破壊を検知した場合削除
    def store?(key, value)
      begin
        @db.transaction(true) do
          if @db.root?(key)
            @logger.debug "Found key and value from #{@file} key: #{key}, value: #{value}, stored value: #{@db[key]}"
            return @db[key] ==value
          else
            @logger.debug "Not found from #{@file} key: #{key}, value: #{value}"
            return false
          end
        end
      rescue Psych::SyntaxError
        @logger.warn 'Trouble in yaml parse. recreate flag file'
        File.delete(@file) if File.exist? @file
        return false
      end
    end

    #指定したkeyにvalueを保存する
    def store(key, value)
      return if store?(key, value)
      begin
        @db.transaction do
          @logger.debug "Store to #{@file} key: #{key}, value: #{value}"
          @db[key] = value
        end
      end
    end

    #指定したkeyに何らかの値があれば true を返す
    def get?(key)
      begin
        @db.transaction(true) do
          if @db.root?(key)
            @logger.debug "Found key from #{@file} key: #{key}"
            return true
          else
            @logger.debug "Not found key from #{@file} key: #{key}"
            return false
          end
        end
      rescue Psych::SyntaxError
        @logger.warn 'Trouble in yaml parse. recreate flag file'
        File.delete(@file) if File.exist? @file
        return false
      end
    end

    #指定したkeyの値を返す
    def get(key)
      begin
        @db.transaction(true) do
          if @db.root?(key)
            @logger.debug "Get value from #{@file} key: #{key}, value: #{@db[key]}"
            return @db[key]
          else
            @logger.debug "Not found key from #{@file} key: #{key}"
            return nil
          end
        end
      end
    end
  end
end