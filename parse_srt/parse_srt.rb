#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

$:.unshift File.dirname(__FILE__)
$:.unshift File.join(File.dirname(__FILE__), 'lib')
require 'parse_srt'

if $0 == __FILE__
  # For debug
  require 'optparse'
  begin
    options = ARGV.getopts('')
    ParseSrt::start options
  rescue SystemExit => e
    exit e.status
  end
end
