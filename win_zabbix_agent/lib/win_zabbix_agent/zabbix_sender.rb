# -*- coding: utf-8 -*-
require 'logger'
require 'open3'
require 'tempfile'

module WinZabbixAgent
  #
  #= zabbix_sender.exe を利用して値を送信する
  #
  #== 参照
  #* github
  #  * miyucy/zabbix-sender.rb[https://gist.github.com/miyucy/1170577#file-zabbix-sender-rb]
  #
  class ZabbixSender
    # Windows 標準 Zabbix Sender 導入先
    DEFAULT_ZBX_SENDER = "C:\\Program Files\\ZABBIX Agent\\zabbix_sender.exe"
    # Windows 標準 Zabbix Agent Conf 導入先
    DEFAULT_ZBX_AGENT_CONF = "C:\\Program Files\\ZABBIX Agent\\zabbix_agentd.conf"

    #logger:: ロガー
    #conf:: Zabbix Agent Conf ファイルパス
    #sender:: Zabbix Sender ファイルパス
    def initialize(logger = nil, conf = nil, sender = nil)
      @logger = logger || Logger.new(STDERR)
      @sender = sender || DEFAULT_ZBX_SENDER
      @conf = conf || DEFAULT_ZBX_AGENT_CONF
      load_conf(@conf)
    end

    #zabbix_sender の -i オプションを利用した一括送信を行う
    #logger:: ロガー
    #conf:: Zabbix Agent Conf ファイルパス
    #sender:: Zabbix Sender ファイルパス
    #
    # ZabbixSender.send do
    #   multi_send userkey1, value1
    #   multi_send userkey2, value2
    #   multi_send userkey3, value3
    # end
    #
    def self.send(logger = nil, conf = nil, sender = nil, &blk)
      s = new logger, conf, sender
      s.to &blk
    end

    def to(&blk)
      raise ArgumentError, 'need block' unless block_given?
      tmp_file = Tempfile.open(['tmp_zabbix_sender', '.txt'])
      begin
        @multi_send_value = {}
        @strict = false
        @self_before_instance_eval = eval 'self', blk.binding
        instance_eval &blk
        unless @multi_send_value.empty?
          @logger.info "Send(multi): #{@multi_send_value.length} items"
          @multi_send_value.each do |key, value|
            tmp_file.puts "#@hostname #{key} #{value}"
          end
          tmp_file.close
          cmd = "\"#@sender\" -z #@server -c \"#@conf\" -i \"#{tmp_file.path}\""
          exitstatus, out, err = commandline(cmd)
          error_log = lambda {
            @logger.error '[cmd   ] ' + cmd
            out.each_line { |line| @logger.error '[stdout] '+line.chomp }
            err.each_line { |line| @logger.error '[stderr] '+line.chomp }
          }
          # ZabbixSender の返り値が0以外の場合エラーとする
          if exitstatus > 0
            error_log.call()
            raise StandardError, "zabbix_sender return status(#{exitstatus})."
          end
          # strict mode では送信した値がZabbixServerで受領されることを確認する
          if @strict
            processed, failed, total = nil, nil, nil
            out.each_line { |line|
              processed, failed, total = $1.to_i, $2.to_i, $3.to_i if  line =~ /Processed (\d+) Failed (\d+) Total (\d+)/
            }
            if processed.nil? or failed.nil? or total.nil?
              error_log.call()
              raise StandardError, "zabbix_sender stdout parse error."
            elsif @multi_send_value.size != processed
              error_log.call()
              raise StandardError, "zabbix_sender send status error: #{processed}/#{failed}/#{total}(processed/failed/total)"
            end
          end
        end
      ensure
        @multi_send_value = nil
        @logger.debug "Delete tmp file: #{tmp_file.path}"
        tmp_file.close(true)
      end
    end

    def method_missing(method, *args, &block)
      @self_before_instance_eval.send method, *args, &block
    end

    # See ZabbixSender.send
    def multi_send(*args)
      key = args.shift
      value = args.shift
      @logger.debug "Send(multi): #@hostname #{key} #{value}"
      @multi_send_value[key] = value
    end

    def strict(b)
      pre = @strict
      @strict = b
      @logger.debug "Send(multi): Strict mode set [#{b}] from [#{pre}]" unless pre == b
    end

    #zabbix_sender の -k -o オプションを利用した送信を行う
    #
    # ZabbixSender.new.send userkey1, value1
    #
    def send(*args)
      key = args.shift
      value = args.shift
      @logger.debug "Send: #{key}, #{value}"
      if value.instance_of?(String)
        cmd = "\"#@sender\" -z #@server -c \"#@conf\" -k #{key} -o \"#{value}\""
      else
        cmd = "\"#@sender\" -z #@server -c \"#@conf\" -k #{key} -o #{value}"
      end
      commandline(cmd)
    end

    def commandline(cmd)
      @logger.debug "[cmd   ] #{cmd}"
      out, err, status = Open3.capture3(cmd)
      out.each_line { |line| @logger.debug '[stdout] '+line.chomp }
      err.each_line { |line| @logger.warn '[stderr] '+line.chomp }
      @logger.debug "[exit  ] #{status.exitstatus}"
      [status.exitstatus, out, err]
    end

    #Zabbix Agent の設定ファイルを読みこむ
    #
    #path:: 設定ファイルのパス
    def load_conf(path)
      return unless FileTest.exist? path
      File.open(path, 'rb') { |f|
        f.readlines.each { |line|
          line.gsub!(/#.*$/, '')
          if line =~ /(Server(?:Port)?)\s*=\s*([0-9a-zA-Z\-_\.]+)\s*/
            key, value = $1, $2
            @server = value if key == 'Server'
            @port = value if key == 'ServerPort'
          elsif line =~ /(Hostname)\s*=\s*([0-9a-zA-Z\-_\.]+)\s*/
            key, value = $1, $2
            @hostname = value if key == 'Hostname'
          end
        }
      }
    end
    attr_reader :multi_send_value
  end
end
