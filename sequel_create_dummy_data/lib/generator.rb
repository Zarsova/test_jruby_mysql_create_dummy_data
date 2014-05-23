# -*- coding: utf-8 -*-
#!/usr/bin/env ruby

def gen_loop_file(file_values, skip=0)
  gen_loop_array(file_values, skip)
end

def gen_fixnum(from)
  counter = 0
  lambda do
    out = from + counter
    counter += 1
    out
  end
end

def gen_loop_string(n, range_object, skip = 0)
  begin_address = range_object.begin
  end_address = range_object.end
  current_address = begin_address.clone
  first = true
  lambda do
    if first
      first = false
      skip.times.each {
        current_address.succ!
        if current_address > end_address
          current_address = begin_address.clone
        end
      }
    else
      n.times.each {
        current_address.succ!
      }
      if current_address > end_address
        current_address = begin_address.clone
      end
    end
    current_address.clone
  end
end

def gen_loop_fixnum(n, range_object, skip = 0)
  start_num = range_object.begin # 1
  end_num = range_object.end # 3
  offset = 0
  first = true
  lambda do
    if first
      first = false
      skip.times.each {
        offset += n
        if offset > (end_num - start_num)
          offset = 0
        end
      }
    else
      offset += n
      if offset > (end_num - start_num)
        offset = 0
      end
    end
    start_num + offset # 1 % 3
  end
end

def gen_loop_range(n, range_object, skip = 0)
  if range_object.begin.instance_of?(String)
    gen_loop_string(n, range_object, skip)
  else
    gen_loop_fixnum(n, range_object, skip)
  end
end

def gen_loop_array(value, skip = 0)
  size = value.size
  count = skip % size
  lambda do
    out = value[count]
    count += 1
    if count >= size
      count = 0
    end
    out
  end
end

def gen_int(offset, cycle =10**length)
  current = offset ||=0
  lambda do
    out = current % cycle
    current += 1
    out
  end
end

def gen_char(offset, character_maximum_length)
  current = offset ||=0
  lambda do
    out = current.to_s.rjust(character_maximum_length, "0")[-character_maximum_length..-1]
    current += 1
    out
  end
end

def gen_bit(offset)
  current = offset ||=0
  lambda do
    out = "b'#{current % 2}'".lit
    current += 1
    out
  end
end

def get_column_func(data_type, length, character_maximum_length, start_num = 0)
  if data_type == 'int' || data_type == 'smallint'|| data_type == 'bigint' ||
      data_type == 'tinyint' || data_type == 'decimal' || data_type == 'double'
    if data_type.index('smallint') != nil
      func = gen_int(start_num, 30000)
    elsif data_type == 'int' && length > 9
      func = gen_int(start_num, 2000000000)
    elsif data_type == 'double'
      func = gen_int(start_num, 2000000000)
    else
      func = gen_int(start_num, 10**length)
    end
  elsif data_type == 'timestamp'
    func = lambda do
      Sequel.lit 'CURRENT_TIMESTAMP'
    end
  elsif data_type == 'datetime'
    func = lambda do
      Sequel.lit 'NOW()'
    end
  elsif data_type == 'time'
    func = lambda do
      Sequel.lit 'CURRENT_TIME'
    end
  elsif data_type == 'date'
    func = lambda do
      Sequel.lit 'CURRENT_DATE'
    end
  elsif data_type == 'char' || data_type == 'varchar'
    func = gen_char(start_num, character_maximum_length)
  elsif data_type == 'text' || data_type == 'longtext'
    func = lambda do
      '01234567890123456789012345678901234567890123456789'
    end
  elsif data_type.index('bit')!= nil
    func = gen_bit(start_num)
  else
    raise Exception, "must not happen"
  end
  func
end

