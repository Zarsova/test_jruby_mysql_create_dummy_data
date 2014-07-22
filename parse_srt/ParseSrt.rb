# -*- coding: utf-8 -*-
$:.unshift File.dirname(__FILE__)
$:.unshift File.join(File.dirname(__FILE__), 'lib')
require 'logger'

class ParseSrt

  def initialize(logger = nil)
    @logger = logger || Logger.new(STDERR)
    @logger.progname = 'ParseSrt'
  end


  def self.run(params)
    start = Time.now
    begin
      logger = Logger.new(STDERR); logger.level = Logger::DEBUG
      ParseSrt::new(logger).main
    ensure
      logger.info("Seconds spent #{Time.now - start}")
    end
    exit 0
  end
end

if $0 == __FILE__
  # For debug
  require 'optparse'
  begin
    params = ARGV.getopts('')
    ParseSrt.run params
  rescue SystemExit => e
    exit e.status
  end
end
