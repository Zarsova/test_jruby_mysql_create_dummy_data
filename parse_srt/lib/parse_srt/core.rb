#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

module ParseSrt
  module Core
    def inits
      @inits ||= []
    end

    def init(&block)
      inits << block
    end

    def mutex
      @mutex ||= Mutex.new
    end

    def sync(&block)
      mutex.synchronize do
        block.call
      end
    end

    def input(text)
      system("cls")
      puts command(text)
      if command = command(text)
        command[:block].call()
      end
    end

    def start(options = {})
      _start = Time.now
      begin
        EM.run do
          Readline.basic_word_break_characters = " \t\n\"\\'`$><=;|&{(@"
          while buf = Readline.readline("> ", true)
            sync {
              input(buf.strip)
            }
          end
          EM.stop_event_loop
        end
      ensure
        puts "Seconds spent #{Time.now - _start}"
      end
      exit 0
    end

    def stop
      EventMachine.stop_event_loop
    end
  end
  extend Core
end
